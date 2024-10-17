use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self};
use std::process::ExitCode;
use std::ops::Not;
use std::io::{self, Cursor, Read};
use std::fs::File;
use std::collections::{BTreeMap, BTreeSet, LinkedList};
use std::time::Duration;

use tracing::{field, info_span};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

fn main() -> ExitCode {

    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE | FmtSpan::NEW)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    if let Some(filename) = std::env::args().nth(1) {

        let _enter = info_span!("TOTAL");

        let file = File::open(filename).unwrap();

        let buffer_size = 1 << 22;

        let mut batches = Batches::with_buffer_size(file, buffer_size);

        let batch_queue: Arc<Mutex<LinkedList<Vec<u8>>>>  = Arc::new(Mutex::new(LinkedList::new()));
        let table_queue: Arc<Mutex<LinkedList<StatsMap>>>  = Arc::new(Mutex::new(LinkedList::new()));
        let condvar = Arc::new(Condvar::new());
        let table_queue_condvar = Arc::new(Condvar::new());
        let worker_count = 6;
        let producer_done = Arc::new(AtomicBool::new(false));
        let working = Arc::new(AtomicUsize::new(worker_count));

        let mut workers = Vec::new();

        for worker_index in 0..worker_count {
            let batch_queue = Arc::clone(&batch_queue);
            let table_queue = Arc::clone(&table_queue);
            let condvar = Arc::clone(&condvar);
            let table_queue_condvar = Arc::clone(&table_queue_condvar);
            let producer_done = Arc::clone(&producer_done);
            let working = Arc::clone(&working);
            let consumer_worker = thread::spawn(move || {

                let mut batch_count = 0;
                let _span = info_span!("worker", i=worker_index, batch_count=field::Empty);

                while ! producer_done.load(Ordering::Relaxed) {
                    let batch = {
                        let guard = batch_queue.lock().unwrap();
                        let mut queue = condvar.wait(guard).unwrap();
                        queue.pop_front()
                    };

                    if let Some(batch) = batch {
                        batch_count += 1;
                        let table = process_batch(batch.as_slice());
                        {
                            let mut table_queue = table_queue.lock().unwrap();
                            table_queue.push_back(table);
                            table_queue_condvar.notify_one();
                        }
                    }
                }

                _span.record("batch_count", batch_count);

                working.fetch_sub(1, Ordering::Relaxed);
                table_queue_condvar.notify_one();

            });
            workers.push(consumer_worker);
        }

        let merge_worker = thread::spawn(move || {
            let _span = info_span!("merge_worker", max_queue_size=field::Empty);
            let mut max_queue_size = 0;
            let result = loop {
                let to_merge = {
                    let guard = table_queue.lock().unwrap();
                    let (mut table_queue, _) = table_queue_condvar.wait_timeout_while(guard, Duration::from_millis(10), |q| q.len() < 2).unwrap();
                    max_queue_size = max_queue_size.max(table_queue.len());

                    let workers_running = working.load(Ordering::Relaxed) > 0;

                    if table_queue.len() > 2 {
                        let t1 = table_queue.pop_front().unwrap();
                        let t2 = table_queue.pop_front().unwrap();
                        Some((t1, t2))
                    } else if workers_running {
                        None
                    } else {
                        break table_queue.pop_front().unwrap()
                    }
                };

                if let Some((table1, table2)) = to_merge {
                    let merged = table1.merge(table2);
                    let mut table_queue = table_queue.lock().unwrap();
                    table_queue.push_back(merged);
                };
            };
            _span.record("max_queue_size", max_queue_size);
            result
        });

        {
            let mut max_queue_size = 0;
            let mut total_batches = 0;
            let _enter = info_span!("producer", max_queue_size = field::Empty, total_batches=field::Empty);
            while let Some(Ok(batch)) = batches.next() {
                total_batches += 1;
                {
                    let mut batch_queue = batch_queue.lock().unwrap();
                    batch_queue.push_back(batch);
                    max_queue_size = max_queue_size.max(batch_queue.len());
                }
                condvar.notify_one();
            }
            producer_done.store(true, Ordering::Relaxed);
            condvar.notify_all();
            _enter.record("max_queue_size", max_queue_size);
            _enter.record("total_batches", total_batches);
        }

        for w in workers {
            w.join().unwrap();
        }

        let final_table = merge_worker.join().unwrap();
        for (key, stats) in final_table.iter() {
            let name = String::from_utf8(key.to_owned()).unwrap();
            println!("{} {:.2} {:.2} {:.2}", name, stats.min, stats.mean(), stats.max);
        }

        ExitCode::SUCCESS
    } else {
        eprintln!("missing filename");
        ExitCode::FAILURE
    }
}

pub fn process_batch(batch: &[u8]) -> StatsMap {
    let mut table = StatsMap::new();
    for entry_data in batch.split(|&c| c == b'\n') {
        if ! entry_data.is_empty() {
            let (key, value) = entry_from_bytes(entry_data).unwrap();
            table.add(key, value);
        }
    }
    table
}

#[allow(unused)]
type MemoryRead = Cursor<Vec<u8>>;

struct Batches<R> {
    file: R,
    buffer: Vec<u8>,
    buffer_start: usize,
}

impl <R: Read> Batches<R> {
    pub fn with_buffer_size(file: R, buffer_size: usize) -> Batches<R> {
        Batches { file, buffer: vec![0; buffer_size], buffer_start: 0 }
    }
}

#[allow(unused)]
impl Batches<MemoryRead> {
    pub fn from_str<S: Into<Vec<u8>>>(data: S, buffer_size: usize) -> Batches<MemoryRead> {
        Batches {
            file: MemoryRead::new(data.into()),
            buffer: vec![0; buffer_size],
            buffer_start: 0,
        }
    }
}

impl <R: Read> Batches<R> {
    fn next_batch(&mut self) -> io::Result<Vec<u8>> {
        let file = &mut self.file;
        let buffer = &mut self.buffer;
        let size = self.buffer_start + file.read(&mut buffer[self.buffer_start..])?;

        // TODO: handle short reads

        if size == 0 {
            Ok(Vec::new())
        } else {
            let buffer = &mut buffer[..size];
            if let Some(last_line_break_index) = buffer.iter().rev().position(|&c| c == b'\n').map(|i| buffer.len() - i) {

                let (left_buffer, right_buffer) = buffer.split_at_mut(last_line_break_index);
                let result = left_buffer.to_owned();
                left_buffer[..right_buffer.len()].copy_from_slice(right_buffer);
                self.buffer_start = right_buffer.len();

                Ok(result)

            } else {
                Err(io::Error::other("buffer size insuficient"))
            }
        }
    }
}

impl <R: Read> Iterator for Batches<R> {
    type Item = io::Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.next_batch();
        result.map(|data| data.is_empty().not().then_some(data)).transpose()
    }
}

#[derive(Debug, Clone)]
pub struct SummaryStat {
    pub min: f32,
    pub max: f32,
    pub sum: f32,
    pub count: f32,
}

impl SummaryStat {
    pub fn new() -> Self {
        SummaryStat { min: f32::MAX, max: f32::MIN, sum: 0.0, count: 0.0 }
    }

    pub fn add(&mut self, value: f32) {
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.sum += value;
        self.count += 1.0;
    }

    pub fn mean(&self) -> f32 { self.sum / self.count }

    fn merge(&mut self, stat: &SummaryStat) {
        self.min = self.min.min(stat.min);
        self.max = self.max.max(stat.max);
        self.sum += stat.sum;
        self.count += stat.count;
    }
}

pub struct EntryCollection {
    //pub entries: Vec<(Vec<u8>, SummaryStat)>,
    //pub entrie_table: HashMap<Vec<u8>, SummaryStat>,
    //pub entry_table: Vec<Option<HashMap<Vec<u8>, SummaryStat>>>,
    pub entry_table: Vec<Option<BTreeMap<Vec<u8>, SummaryStat>>>,
}

impl EntryCollection {
    pub fn new() -> EntryCollection {
        //EntryCollection { entries: Vec::new() }
        //EntryCollection { entries: HashMap::new() }
        let mut entry_table = Vec::new();
        for _ in 0..255 { entry_table.push(None) }
        EntryCollection { entry_table }
    }

    pub fn add(&mut self, new_entry: Entry<&[u8]>) {
        let i = new_entry.0[0] as usize;
        let entries = match &mut self.entry_table[i] {
            Some(map) => map,
            None => {
                self.entry_table[i] = Some(BTreeMap::new());
                self.entry_table[i].as_mut().unwrap()
            }
        };
        if ! entries.contains_key(new_entry.0) {
            entries.insert(new_entry.0.to_owned(), SummaryStat::new());
        }
        entries.get_mut(new_entry.0).unwrap().add(new_entry.1)



        //if ! self.entries.contains_key(new_entry.0) {
        //    self.entries.insert(new_entry.0.to_owned(), SummaryStat::new());
        //}
        //self.entries.get_mut(new_entry.0).unwrap().add(new_entry.1)

        //for entry in self.entries.iter_mut() {
        //    if entry.0 == new_entry.0 {
        //        entry.1.add(new_entry.1);
        //        return;
        //    }
        //}
        //let mut stat = SummaryStat::new();
        //stat.add(new_entry.1);
        //self.entries.push((new_entry.0.to_owned(), stat))
    }
}

pub struct StatsMap {
    pub table: Vec<Vec<(Vec<u8>, SummaryStat)>>,
    pub max_size: usize,
    pub occupied: BTreeSet<usize>
}

impl StatsMap {
    pub fn new() -> StatsMap {
        let table = vec![Vec::with_capacity(16); 256 * 256];
        //let mut table = Vec::new();
        //(0..256 * 256).for_each(|_| table.push(Vec::with_capacity(16)));
        StatsMap { table, max_size: 0, occupied: BTreeSet::new() }
    }

    pub fn get_or_insert_mut<'a>(&'a mut self, key: &'_ [u8]) -> &'a mut SummaryStat {
        let k0 = key[0] as usize;
        let k1 = key[1] as usize;
        let k = (k0 << 8) | k1;

        let slot = &self.table[k];
        if let Some(i) = slot.iter().position(|e| e.0 == key) {
            &mut self.table[k][i].1
        } else {
            let i = self.table[k].len();
            self.table[k].push((key.to_owned(), SummaryStat::new()));
            self.occupied.insert(k);
            self.max_size = self.max_size.max(self.table[k].len());
            &mut self.table[k][i].1
        }
    }

    pub fn add(&mut self, key: &[u8], value: f32) {
        self.get_or_insert_mut(key).add(value);
    }

    pub fn merge_stat(&mut self, key: &[u8], stat: &SummaryStat) {
        self.get_or_insert_mut(key).merge(stat);
    }

    fn merge(mut self, table2: StatsMap) -> StatsMap {
        for ix in table2.occupied {
            for (key, stat) in &table2.table[ix] {
                self.merge_stat(key, stat);
            }
        }
        self
    }

    fn iter(&self) -> StatsMapIter {
        StatsMapIter::new(self)
    }
}

struct StatsMapIter<'a> {
    x: Box<dyn Iterator<Item = &'a(Vec<u8>, SummaryStat)> + 'a>,
}

impl <'a> StatsMapIter<'a> {
    pub fn new(stats_map: &'a StatsMap) -> Self {
        let mut slots: Vec<usize> = stats_map.occupied.iter().cloned().collect();
        slots.sort();
        let iter = slots.into_iter().flat_map(|ix| stats_map.table[ix].iter());
        let x = Box::new(iter);
        Self { x }
    }
}

impl <'a> Iterator for StatsMapIter<'a> {
    type Item = &'a (Vec<u8>, SummaryStat);
    fn next(&mut self) -> Option<Self::Item> { self.x.next() }
}

type Entry<K> = (K, f32);

pub fn entry_from_bytes(bytes: &[u8]) -> Result<Entry<&[u8]>, &'static str> {
    if let Some(separator_index) = bytes.iter().position(|&c| c == b';') {
        let (name, value_data) = bytes.split_at(separator_index);
        //let value_data = &value_data[1..value_data.len()-1];
        //let value_text = String::from_utf8(value_data.to_owned()).map_err(|_e| "invalid entry: from_utf8 error")?;
        //let value: f32 = value_text.parse().map_err(|_e| "invalid entry: parse error")?;
        let value = parse_float(&value_data[1..value_data.len()]);
        Ok((name, value))
    } else {
        Err("invalid entry: does not contain separator ';'")
    }
}

pub fn parse_float(bytes: &[u8]) -> f32 {
    let negative = bytes[0] == b'-';
    let start = if negative { 1 } else { 0 };
    let mut acc: u32 = 0;
    for c in &bytes[start..] {
        if *c != b'.' {
            acc = 10 * acc + (c - b'0') as u32;
        }
    }
    let decimals = if let Some(period_index) = bytes.iter().position(|&c| c == b'.') { bytes.len() - period_index - 1 } else { 1 };
    (if negative { -1.0 } else { 1.0 }) * acc as f32 / (10_i32).pow(decimals as u32) as f32
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn batches_from_empty_str() {
        let mut batches = Batches::from_str("", 8);
        assert!( dbg!(batches.next()).is_none() );
    }

    #[test]
    pub fn batches_from_single_record_exact_buffer_size() {
        let mut batches = Batches::from_str("abc;1.0\n", 8);
        let batch = dbg!(batches.next()).unwrap().unwrap();
        assert_eq!(batch, b"abc;1.0\n");
    }

    #[test]
    pub fn batches_from_single_record_insuficient_buffer_size() {
        let mut batches = Batches::from_str("abc;1.0\n", 5);
        match dbg!(batches.next()) {
            Some(Err(_)) => {},
            next => panic!("unexpected pattern: {:?}", next),
        }
    }

    #[test]
    pub fn batches_from_two_records() {
        let mut batches = Batches::from_str("abc;1.0\nxyz:2.0\n", 32);
        let batch = dbg!(batches.next()).unwrap().unwrap();
        assert_eq!(batch, b"abc;1.0\nxyz:2.0\n");
    }

    #[test]
    pub fn batches_from_two_records_short_buffer() {
        let mut batches = Batches::from_str("abc;1.0\nxyz;2.0\n", 9);

        let batch = dbg!(batches.next()).unwrap().unwrap();
        assert_eq!(batch, b"abc;1.0\n");

        let batch = dbg!(batches.next()).unwrap().unwrap();
        assert_eq!(batch, b"xyz;2.0\n");

        let batch = dbg!(batches.next());
        assert!(batch.is_none());
    }

    #[test]
    pub fn batches_from_two_records_large_buffer() {
        let mut batches = Batches::from_str("abc;1.0\nxyz;2.0\n", 128);

        let batch = dbg!(batches.next()).unwrap().unwrap();
        assert_eq!(batch, b"abc;1.0\nxyz;2.0\n");

        let batch = dbg!(batches.next());
        assert!(batch.is_none());
    }

    #[test]
    pub fn entries_from_bytes() {
        let entries = dbg!(entry_from_bytes(b"abc;1.0\n")).unwrap();
        let name = b"abc".as_slice();
        assert_eq!(entries, (name, 1.0));
    }

    #[test]
    pub fn entries_from_batch() {
        let batch = b"abc;1.0\nxyz;2.0\n";
        let entries: Vec<Entry<&[u8]>> = batch.split(|&c| c == b'\n').filter(|e| !e.is_empty()).map(entry_from_bytes).map(|r| r.unwrap()).collect();
        assert_eq!(entries, vec![(&b"abc"[..], 1.0), (b"xyz", 2.0)]);
    }

    #[test]
    pub fn entry_collection() {
        // let mut collection = EntryCollection::new();
        // collection.add((b"a", -1.0));
        // collection.add((b"a", 1.0));
        // dbg!(collection.entries);
        // assert!(1 == 0);
    }

    #[test]
    pub fn test_parse_float() {
        assert!(parse_float(b"1") - 1.0 < 1e-6);
        assert!(parse_float(b"1.1") - 1.1 < 1e-6);
        assert!(parse_float(b"2.09") - 2.09 < 1e-6);
    }
}

