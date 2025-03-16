CREATE DATABASE final_project;
USE final_project;

-- change table name to match whatever category you're importing metadata for
CREATE TABLE meta_subscription_boxes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    main_category VARCHAR(100),
    title TEXT,
    average_rating FLOAT,
    rating_number INT,
    features JSON,
    description JSON,
    price DECIMAL(10,2),
    images JSON,
    videos JSON,
    store TEXT,
    categories JSON,
    details JSON,
    parent_asin VARCHAR(20) UNIQUE,
    bought_together JSON
);

-- change table name to match whatever category you're importing reviews for
CREATE TABLE subscription_boxes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    rating FLOAT,
    title TEXT,
    `text` TEXT,
    images JSON,
    asin VARCHAR(20),
    parent_asin VARCHAR(20),
    user_id VARCHAR(255),
    timestamp BIGINT,
    verified_purchase TINYINT(1),
    helpful_vote INT,
    INDEX idx_asin (asin),
    INDEX idx_parent_asin (parent_asin),
    INDEX idx_user_id (user_id),
    -- change name of constraint here to match category
    CONSTRAINT fk_subscription_boxes_meta
    -- reference correct name of metadata table here
        FOREIGN KEY (parent_asin) REFERENCES meta_subscription_boxes(parent_asin)
);
