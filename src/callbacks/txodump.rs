use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::hash::BuildHasherDefault;
use std::io::{LineWriter, Write};
use std::path::PathBuf;

use clap::{App, Arg, ArgMatches, SubCommand};
use twox_hash::XxHash;

use callbacks::Callback;
use errors::{OpError, OpResult};

use blockchain::parser::types::CoinType;
use blockchain::proto::block::Block;
use blockchain::proto::tx::TxOutpoint;
use blockchain::proto::ToRaw;

/// Dumps the UTXO set into a CSV file
pub struct TXODump {
    dump_folder: PathBuf,
    txo_writer: LineWriter<File>,
    utxo_set: HashMap<TxOutpoint, (u64, usize), BuildHasherDefault<XxHash>>, // TxOutpoint (K), (Coin Value, Blockheight) (V)
    start_height: usize,
    end_height: usize,
    tx_count: u64,
    in_count: u64,
    out_count: u64,
}

impl TXODump {
    fn create_writer(path: PathBuf) -> OpResult<LineWriter<File>> {
        let file = match OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
        {
            Ok(f) => f,
            Err(err) => return Err(OpError::from(err)),
        };
        Ok(LineWriter::new(file))
    }

    /// Load the UTXO set from an existing CSV file
    fn load_utxo_set(&mut self) -> OpResult<usize> {
        info!("NYI for TXODump");
        //let csv_file_path = self.dump_folder.join("utxo.csv");
        //let csv_file_path_string = csv_file_path.as_path().to_str().unwrap();
        //let csv_file = match CsvFile::new(csv_file_path.to_owned(), b';') {
        //    Ok(idx) => idx,
        //    Err(e) => {
        //        return Err(tag_err!(
        //            e,
        //            "Unable to load UTXO CSV file {}!",
        //            csv_file_path_string
        //        ))
        //    }
        //};

        //Ok(self.utxo_set.len())
        Ok(0)
    }
}

impl Callback for TXODump {
    fn build_subcommand<'a, 'b>() -> App<'a, 'b>
    where
        Self: Sized,
    {
        SubCommand::with_name("txodump")
            .about("Dumps the spent transaction outputs into a CSV file")
            .version("0.1")
            .author("RY")
            .arg(
                Arg::with_name("dump-folder")
                    .help("Folder to store the CSV file")
                    .index(1)
                    .required(true),
            )
    }

    fn new(matches: &ArgMatches) -> OpResult<Self>
    where
        Self: Sized,
    {
        let ref dump_folder = PathBuf::from(matches.value_of("dump-folder").unwrap());
        match (|| -> OpResult<Self> {
            let cb = TXODump {
                dump_folder: PathBuf::from(dump_folder),
                txo_writer: TXODump::create_writer(dump_folder.join("txo.csv.tmp"))?,
                utxo_set: Default::default(),
                start_height: 0,
                end_height: 0,
                tx_count: 0,
                in_count: 0,
                out_count: 0,
            };
            Ok(cb)
        })() {
            Ok(s) => return Ok(s),
            Err(e) => {
                return Err(tag_err!(
                    e,
                    "Couldn't initialize TXODump with folder: `{:#?}`",
                    dump_folder.as_path()
                ))
            }
        }
    }

    fn on_start(&mut self, _: CoinType, block_height: usize) {
        self.start_height = block_height;
        info!(target: "TXODump [on_start]", "Using `TXODump` with dump folder: {:?} and start block {}...", &self.dump_folder, self.start_height);
        match self.load_utxo_set() {
            Ok(utxo_count) => {
                info!(target: "TXODump [on_start]", "Loaded {} UTXOs.", utxo_count);
            }
            Err(_) => {
                info!(target: "TXODump [on_start]", "No previous UTXO loaded.");
            }
        }
    }

    fn on_block(&mut self, block: Block, block_height: usize) {
        debug!(target: "TXODump [on_block]", "Block: {}.", block_height);

        for tx in block.txs {
            self.in_count += tx.value.in_count.value;
            self.out_count += tx.value.out_count.value;

            // Transaction inputs
            for input in &tx.value.inputs {
                let tx_outpoint = TxOutpoint {
                    txid: input.outpoint.txid,
                    index: input.outpoint.index,
                };

                // coinbase txinput has previous index of 0xFFFFFFFF
                if tx_outpoint.index == 0xFFFFFFFF {
                    continue;
                }

                trace!(target: "TXODump [on_block] [TX inputs]", "Removing {:#?} from UTXO set.", tx_outpoint);
                // Write TXOStat
                {
                    let feerate = tx.value.get_fees(&self.utxo_set) / tx.value.to_bytes().len() as u64;
                    match self.utxo_set.get(&tx_outpoint) {
                        Some((utxo_val, utxo_height)) => {
                            let coinage = block_height - utxo_height;
                            self.txo_writer
                                .write_all(
                                    format!(
                                        "{};{};{};{}\n",
                                        block_height, coinage, feerate, utxo_val
                                    )
                                    .as_bytes(),
                                )
                                .unwrap();
                            self.utxo_set.remove(&tx_outpoint);
                        }
                        _ => {}
                    }
                }
            }

            // Transaction outputs
            for (i, output) in tx.value.outputs.iter().enumerate() {
                let tx_outpoint = TxOutpoint {
                    txid: tx.hash,
                    index: i as u32,
                };
                let coin_value = output.out.value;

                trace!(target: "TXODump [on_block] [TX outputs]", "Adding UTXO {:#?} to the UTXO set.", tx_outpoint);
                self.utxo_set
                    .insert(tx_outpoint, (coin_value, block_height));
            }
        }
        self.tx_count += block.tx_count.value;
    }

    fn on_complete(&mut self, _block_height: usize) {
        // Rename temp files
        fs::rename(
            self.dump_folder.as_path().join("txo.csv.tmp"),
            self.dump_folder.as_path().join("txo.csv"),
        )
        .expect("Unable to rename tmp file!");

        info!(target: "TXODump [on_complete]", "Done.\nDumped all {} blocks:\n\
                                   \t-> transactions: {:9}\n\
                                   \t-> inputs:       {:9}\n\
                                   \t-> outputs:      {:9}",
             self.end_height + 1, self.tx_count, self.in_count, self.out_count);
    }
}
