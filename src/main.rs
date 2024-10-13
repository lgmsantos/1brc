use std::thread;
use std::process::ExitCode;
use std::ops::Not;
use std::io::{self, Cursor, Read};
use std::fs::File;
use std::collections::{BTreeMap, HashMap};

use tracing::info_span;
use tracing_subscriber::fmt::format::FmtSpan;

fn main() -> ExitCode {

    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .init();

    if let Some(filename) = std::env::args().nth(1) {

        let file = File::open(filename).unwrap();

        let buffer_size = 1 << 21;

        let mut batches = Batches::with_buffer_size(file, buffer_size);

        let mut handles = Vec::new();

        let mut worker = 0;

        info_span!("spawn consumers", buffer_size=format!("{:.2}MB", buffer_size / 1024 / 1024)).in_scope(|| {
            while let Some(Ok(batch)) = batches.next() {
                worker += 1;
                let worker = worker;
                let handle = thread::spawn(move || {
                    let mut entry_collection = EntryCollection::new();
                    {
                        let _enter = if worker % 1000 == 0 { Some(info_span!("worker", worker=worker)) } else { None };
                        let batch = batch;
                        for entry in batch.split(|&c| c == b'\n').filter(|b| !b.is_empty()).map(entry_from_bytes).map(|r| r.unwrap()) {
                            entry_collection.add(entry);
                        }
                    }
                    entry_collection
                });
                handles.push(handle);
            }
        });


        {
            let _enter = info_span!("join handles", handle_count=handles.len());
            for handle in handles {
                handle.join().unwrap();
            }
        }

        ExitCode::SUCCESS

    } else {
        eprintln!("missing filename");
        ExitCode::FAILURE
    }
}

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

#[derive(Debug)]
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

type Entry<K> = (K, f32);

pub fn entry_from_bytes(bytes: &[u8]) -> Result<Entry<&[u8]>, &'static str> {
    if let Some(separator_index) = bytes.iter().position(|&c| c == b';') {
        let (name, value_data) = bytes.split_at(separator_index);
        //let value_data = &value_data[1..value_data.len()-1];
        //let value_text = String::from_utf8(value_data.to_owned()).map_err(|_e| "invalid entry: from_utf8 error")?;
        //let value: f32 = value_text.parse().map_err(|_e| "invalid entry: parse error")?;
        let value = parse_float(&value_data[1..value_data.len()-1]);
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
}
