use std::{io::{self, Cursor, Read}, ops::Not, process::ExitCode};

fn main() -> ExitCode {
    if let Some(filename) = std::env::args().nth(1) {
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

impl Batches<MemoryRead> {
    pub fn from_str(buffer_size: usize, data: String) -> Batches<MemoryRead> {
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn batches_from_empty_str() {
        let mut batches = Batches::from_str(8, String::from(""));
        assert!( dbg!(batches.next()).is_none() );
    }

    #[test]
    pub fn batches_from_single_record_exact_buffer_size() {
        let mut batches = Batches::from_str(8, String::from("abc;1.0\n"));
        match dbg!(batches.next()) {
            Some(Ok(batch)) => {
                let text = String::from_utf8(batch).unwrap();
                assert_eq!(text, "abc;1.0\n")
            },
            next => panic!("unexpected pattern: {:?}", next),
        }
    }

    #[test]
    pub fn batches_from_single_record_insuficient_buffer_size() {
        let mut batches = Batches::from_str(5, String::from("abc;1.0\n"));
        match dbg!(batches.next()) {
            Some(Err(_)) => {},
            next => panic!("unexpected pattern: {:?}", next),
        }
    }

    #[test]
    pub fn batches_from_two_records() {
        let mut batches = Batches::from_str(32, "abc;1.0\nxyz:2.0\n".to_owned());
        match dbg!(batches.next()) {
            Some(Ok(batch)) => {
                let text = String::from_utf8(batch).unwrap();
                assert_eq!(text, "abc;1.0\nxyz:2.0\n")
            },
            next => panic!("unexpected pattern: {:?}", next),
        }
    }

    #[test]
    pub fn batches_from_two_records_short_buffer() {
        let mut batches = Batches::from_str(9, "abc;1.0\nxyz;2.0\n".to_owned());
        match dbg!(batches.next()) {
            Some(Ok(batch)) => {
                let text = String::from_utf8(batch).unwrap();
                assert_eq!(text, "abc;1.0\n")
            },
            next => panic!("unexpected pattern: {:?}", next),
        }

        match dbg!(batches.next()) {
            Some(Ok(batch)) => {
                let text = String::from_utf8(batch).unwrap();
                assert_eq!(text, "xyz;2.0\n")
            },
            next => panic!("unexpected pattern: {:?}", next),
        }
    }

}
