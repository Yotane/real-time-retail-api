CREATE TABLE IF NOT EXISTS stores (
    store_id    VARCHAR(10)     PRIMARY KEY,
    region      VARCHAR(50)     NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    product_id  VARCHAR(10)     PRIMARY KEY,
    category    VARCHAR(50)     NOT NULL
);

CREATE TABLE IF NOT EXISTS calendar (
    date                DATE            PRIMARY KEY,
    weather_condition   VARCHAR(20),
    is_holiday_promo    TINYINT(1)      NOT NULL DEFAULT 0,
    seasonality         VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS sales_facts (
    id                  BIGINT          PRIMARY KEY AUTO_INCREMENT,
    date                DATE            NOT NULL,
    store_id            VARCHAR(10)     NOT NULL,
    product_id          VARCHAR(10)     NOT NULL,
    inventory_level     INT,
    units_sold          INT,
    units_ordered       INT,
    demand_forecast     DECIMAL(10, 2),
    price               DECIMAL(10, 2),
    discount            DECIMAL(5, 2),
    competitor_pricing  DECIMAL(10, 2),
    FOREIGN KEY (date)          REFERENCES calendar(date),
    FOREIGN KEY (store_id)      REFERENCES stores(store_id),
    FOREIGN KEY (product_id)    REFERENCES products(product_id),
    UNIQUE INDEX idx_unique_sale (date, store_id, product_id),
    INDEX idx_date          (date),
    INDEX idx_store         (store_id),
    INDEX idx_product       (product_id),
    INDEX idx_store_product (store_id, product_id)
);

CREATE TABLE IF NOT EXISTS kafka_events (
    id              BIGINT          PRIMARY KEY AUTO_INCREMENT,
    event_id        VARCHAR(36)     NOT NULL,
    store_id        VARCHAR(10)     NOT NULL,
    product_id      VARCHAR(10)     NOT NULL,
    date            DATE            NOT NULL,
    units_sold      INT,
    price           DECIMAL(10, 2),
    discount        DECIMAL(5, 2),
    is_holiday_promo TINYINT(1)     DEFAULT 0,
    weather         VARCHAR(20),
    received_at     TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    UNIQUE INDEX idx_event_id       (event_id),
    INDEX idx_kafka_store   (store_id),
    INDEX idx_kafka_product (product_id),
    INDEX idx_kafka_date    (date)
);
USE retaildb;
CREATE TABLE IF NOT EXISTS optuna_trials (
    id              BIGINT       PRIMARY KEY AUTO_INCREMENT,
    study_name      VARCHAR(100) NOT NULL,
    trial_number    INT          NOT NULL,
    store_id        VARCHAR(10),
    product_id      VARCHAR(10),
    _window          INT,
    min_periods     INT,
    trend_window    INT,
    rmse            DECIMAL(10,4),
    status          VARCHAR(20)  DEFAULT 'complete',
    completed_at    TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_study   (study_name),
    INDEX idx_product (product_id)
);