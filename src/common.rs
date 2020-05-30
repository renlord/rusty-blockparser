extern crate chrono;

pub mod logger {
    use common::chrono::prelude::*;
    use std::io::{stderr, stdout, Write};

    use log::{self, LogLevel, LogLevelFilter, LogMetadata, LogRecord, SetLoggerError};

    pub struct SimpleLogger {
        log_filter: LogLevelFilter,
    }

    impl SimpleLogger {
        pub fn init(log_filter: LogLevelFilter) -> Result<(), SetLoggerError> {
            log::set_logger(|max_log_level| {
                max_log_level.set(log_filter);
                Box::new(SimpleLogger {
                    log_filter: log_filter,
                })
            })
        }

        fn create_log_line(&self, record: &LogRecord) -> String {
            let now = Local::now();
            format!(
                "[{}] {} - {}: {}\n",
                now.to_string(),
                record.level(),
                record.target(),
                record.args()
            )
        }
    }

    impl log::Log for SimpleLogger {
        fn enabled(&self, metadata: &LogMetadata) -> bool {
            metadata.level() <= self.log_filter
        }

        fn log(&self, record: &LogRecord) {
            if self.enabled(record.metadata()) {
                let line = self.create_log_line(record);
                match record.level() {
                    LogLevel::Error => {
                        stderr().write_all(line.as_bytes()).unwrap();
                    }
                    _ => {
                        stdout().write_all(line.as_bytes()).unwrap();
                    }
                }
            }
        }
    }
}
