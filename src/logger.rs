use chrono::Local;
use rust_xlsxwriter::*;
use serde::Serialize;
use std::sync::Mutex;
use tracing::{error, info};

#[derive(Clone, Serialize, Debug)]
pub struct TradeRecord {
    pub timestamp: String,
    pub symbol: String,
    pub side: String,
    pub leverage: f64,
    pub invested_usdt: f64,
    pub notional_usdt: f64,
    pub quantity: f64,
    pub entry_price: f64,
    pub exit_price: f64,
    pub duration_seconds: i64,
    pub gross_pnl_usdt: f64,
    pub fees_usdt: f64,
    pub net_pnl_usdt: f64,
    pub pnl_percent: f64,
    pub exit_reason: String,
}

pub struct ExcelLogger {
    records: Mutex<Vec<TradeRecord>>,
    file_path: String,
}

impl ExcelLogger {
    pub fn new(file_path: &str) -> Self {
        Self {
            records: Mutex::new(Vec::new()),
            file_path: file_path.to_string(),
        }
    }

    pub fn log_trade(&self, record: TradeRecord) {
        let mut recs = self.records.lock().unwrap();
        recs.push(record);
        if let Err(e) = self.export_to_excel(&recs) {
            error!("Failed to export trades to Excel: {}", e);
        }
    }

    fn export_to_excel(&self, records: &[TradeRecord]) -> Result<(), XlsxError> {
        let mut workbook = Workbook::new();
        let worksheet = workbook.add_worksheet();

        // Formats
        let header_format = Format::new()
            .set_bold()
            .set_background_color(Color::RGB(0x1F497D))
            .set_font_color(Color::White)
            .set_align(FormatAlign::Center);

        let currency_format = Format::new().set_num_format("$#,##0.00");
        let crypto_format = Format::new().set_num_format("0.000000");
        let center_format = Format::new().set_align(FormatAlign::Center);
        let green_format = Format::new()
            .set_font_color(Color::RGB(0x008000))
            .set_num_format("$#,##0.00");
        let red_format = Format::new()
            .set_font_color(Color::RGB(0xFF0000))
            .set_num_format("$#,##0.00");
        let green_pct = Format::new()
            .set_font_color(Color::RGB(0x008000))
            .set_num_format("0.00%");
        let red_pct = Format::new()
            .set_font_color(Color::RGB(0xFF0000))
            .set_num_format("0.00%");

        // Headers
        let headers = [
            "Timestamp",
            "Symbol",
            "Side",
            "Leverage",
            "Invested Margin ($)",
            "Notional Size ($)",
            "Quantity (BTC)",
            "Entry Price ($)",
            "Exit Price ($)",
            "Duration (s)",
            "Gross PnL ($)",
            "Binance Fee ($)",
            "Net PnL ($)",
            "Return (PnL %)",
            "Exit Reason",
        ];

        for (col, header) in headers.iter().enumerate() {
            worksheet.write_string_with_format(0, col as u16, *header, &header_format)?;
        }

        // Data Rows
        for (row_idx, r) in records.iter().enumerate() {
            let row = (row_idx + 1) as u32;

            worksheet.write_string_with_format(row, 0, &r.timestamp, &center_format)?;
            worksheet.write_string_with_format(row, 1, &r.symbol, &center_format)?;
            worksheet.write_string_with_format(row, 2, &r.side, &center_format)?;
            worksheet.write_string_with_format(
                row,
                3,
                &format!("{:.0}x", r.leverage),
                &center_format,
            )?;
            worksheet.write_number_with_format(row, 4, r.invested_usdt, &currency_format)?;
            worksheet.write_number_with_format(row, 5, r.notional_usdt, &currency_format)?;
            worksheet.write_number_with_format(row, 6, r.quantity, &crypto_format)?;
            worksheet.write_number_with_format(row, 7, r.entry_price, &currency_format)?;
            worksheet.write_number_with_format(row, 8, r.exit_price, &currency_format)?;
            worksheet.write_number_with_format(row, 9, r.duration_seconds as f64, &center_format)?;

            let gross_fmt = if r.gross_pnl_usdt >= 0.0 {
                &green_format
            } else {
                &red_format
            };
            worksheet.write_number_with_format(row, 10, r.gross_pnl_usdt, gross_fmt)?;

            worksheet.write_number_with_format(row, 11, r.fees_usdt, &currency_format)?;

            let net_fmt = if r.net_pnl_usdt >= 0.0 {
                &green_format
            } else {
                &red_format
            };
            worksheet.write_number_with_format(row, 12, r.net_pnl_usdt, net_fmt)?;

            let pct_fmt = if r.pnl_percent >= 0.0 {
                &green_pct
            } else {
                &red_pct
            };
            worksheet.write_number_with_format(row, 13, r.pnl_percent / 100.0, pct_fmt)?;

            worksheet.write_string_with_format(row, 14, &r.exit_reason, &center_format)?;
        }

        worksheet.autofit();
        workbook.save(&self.file_path)?;
        info!("Saved trade log to Excel file: {}", self.file_path);
        Ok(())
    }
}

pub fn get_current_timestamp() -> String {
    Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
}
