
CREATE TABLE IF NOT EXISTS stocks (
    code VARCHAR(10) PRIMARY KEY,      
    company_name VARCHAR(255),
    isin_code VARCHAR(15)        
);


CREATE TABLE IF NOT EXISTS daily_prices (
    record_date DATE NOT NULL,
    code VARCHAR(10) NOT NULL REFERENCES stocks(code),
    
    close_price DECIMAL(10, 2),
    high_price DECIMAL(10, 2),
    low_price DECIMAL(10, 2),
    open_price DECIMAL(10, 2),
    
    PRIMARY KEY (record_date, code)
);


CREATE TABLE IF NOT EXISTS quarterly_financials (
    record_date DATE NOT NULL,
    code VARCHAR(10) NOT NULL REFERENCES stocks(code),
    
    pe_ratio DECIMAL(10, 2),
    price_to_book DECIMAL(10, 2),
    ev_ebitda DECIMAL(10, 2),
    
    revenue BIGINT,
    net_income BIGINT,
    ebitda BIGINT,
    market_cap BIGINT,
    shares_outstanding BIGINT,
    
    eps DECIMAL(10, 4),
    book_value_per_share DECIMAL(10, 2),
    dividend_yield_pct DECIMAL(10, 2),
    
    gross_profit_margin_pct DECIMAL(10, 2),
    roa_pct DECIMAL(10, 2),
    roe_pct DECIMAL(10, 2),
    
    beta DECIMAL(10, 2),
    one_year_change_pct DECIMAL(10, 2),
    next_earnings_date DATE,
    graham_number DECIMAL(10, 2),
    
    PRIMARY KEY (record_date, code)
);



CREATE TABLE IF NOT EXISTS calculated_indicators(

    record_date DATE NOT NULL,
    code VARCHAR(20) NOT NULL REFERENCES stocks(code),
    
    rsi_14 DECIMAL(10, 2),
    
    sma_50 DECIMAL(10, 2),
    sma_200 DECIMAL(10, 2),
    
    macd_line DECIMAL(10, 2),
    macd_signal DECIMAL(10, 2),
    macd_hist DECIMAL(10, 2),
    
    bb_upper DECIMAL(10, 2),
    bb_lower DECIMAL(10, 2),
    bb_middle DECIMAL(10, 2),
    
    atr_14 DECIMAL(10, 2),

    stoch_k DECIMAL(10, 2),
    stoch_d DECIMAL(10, 2),
    
    PRIMARY KEY (record_date, code)
);