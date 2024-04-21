----------------- CDM --------------------- 

DROP TABLE IF EXISTS cdm.user_product_counters ;

CREATE TABLE cdm.user_product_counters (
	id serial PRIMARY KEY,
	user_id uuid NOT null,
	product_id uuid NOT null ,
	product_name varchar(255) NOT null,
	order_cnt int NOT null CHECK ((order_cnt >= 0)),
	constraint unique_user_product UNIQUE(user_id, product_id)
)

DROP TABLE IF EXISTS cdm.user_category_counters ;

CREATE TABLE cdm.user_category_counters (
	id serial PRIMARY KEY,
	user_id uuid NOT null,
	category_id uuid NOT null ,
	category_name varchar(255) NOT null,
	order_cnt int NOT null CHECK ((order_cnt >= 0)),
	constraint unique_user_category UNIQUE(user_id, category_id)
)

----------------- STG ---------------------

DROP TABLE IF EXISTS stg.order_events ;

CREATE TABLE stg.order_events (
	id serial PRIMARY KEY,
	object_id int unique NOT null,
	payload json NOT null,
	object_type varchar(255) NOT null,
	sent_dttm timestamp NOT null
)

----------------- DDS ---------------------

DROP TABLE IF EXISTS dds.h_user ;

CREATE TABLE dds.h_user (
	h_user_pk uuid PRIMARY KEY,
	user_id varchar(255) NOT null,
	load_dt timestamp NOT null,
	load_src varchar(255) NOT null DEFAULT 'orders-system-kafka'
)

DROP TABLE IF EXISTS dds.h_product ;

CREATE TABLE dds.h_product (
	h_product_pk uuid PRIMARY KEY,
	product_id varchar(255) NOT null,
	load_dt timestamp NOT null,
	load_src varchar(255) NOT null DEFAULT 'orders-system-kafka'
)

DROP TABLE IF EXISTS dds.h_category ;

CREATE TABLE dds.h_category (
	h_category_pk uuid PRIMARY KEY,
	category_name varchar NOT NULL,
	load_dt timestamp NOT null,
	load_src varchar(255) NOT null DEFAULT 'orders-system-kafka'
)


DROP TABLE IF EXISTS dds.h_restaurant ;

CREATE TABLE dds.h_restaurant (
	h_restaurant_pk uuid PRIMARY KEY,
	restaurant_id varchar(255) NOT null,
	load_dt timestamp NOT null,
	load_src varchar(255) NOT null DEFAULT 'orders-system-kafka'
)


DROP TABLE IF EXISTS dds.h_order ;

CREATE TABLE dds.h_order (
	h_order_pk uuid PRIMARY KEY,
	order_id int NOT null,
	load_dt timestamp NOT null,
	order_dt timestamp NOT NULL,
	load_src varchar(255) NOT null DEFAULT 'orders-system-kafka'
)


DROP TABLE IF EXISTS dds.l_order_product ;

CREATE TABLE dds.l_order_product (
	hk_order_product_pk uuid PRIMARY KEY,
	h_order_pk uuid NOT NULL references dds.h_order(h_order_pk),
	h_product_pk uuid NOT NULL references dds.h_product(h_product_pk),
	load_dt timestamp NOT NULL,
	load_src varchar(255) NOT null DEFAULT 'orders-system-kafka'
)


DROP TABLE IF EXISTS dds.l_product_restaurant ;

CREATE TABLE dds.l_product_restaurant (
	hk_product_restaurant_pk uuid PRIMARY KEY,
	h_restaurant_pk uuid NOT NULL references dds.h_restaurant(h_restaurant_pk),
	h_product_pk uuid NOT NULL references dds.h_product(h_product_pk),
	load_dt timestamp NOT NULL,
	load_src varchar(255) NOT null DEFAULT 'orders-system-kafka'
)


DROP TABLE IF EXISTS dds.l_product_category ;

CREATE TABLE dds.l_product_category (
	hk_product_category_pk uuid PRIMARY KEY,
	h_category_pk uuid NOT NULL references dds.h_category(h_category_pk),
	h_product_pk uuid NOT NULL references dds.h_product(h_product_pk),
	load_dt timestamp NOT NULL,
	load_src varchar(255) NOT null DEFAULT 'orders-system-kafka'
)


DROP TABLE IF EXISTS dds.l_order_user ;

CREATE TABLE dds.l_order_user (
	hk_order_user_pk uuid PRIMARY KEY,
	h_order_pk uuid NOT NULL references dds.h_order(h_order_pk),
	h_user_pk uuid NOT NULL references dds.h_user(h_user_pk),
	load_dt timestamp NOT NULL,
	load_src varchar(255) NOT null DEFAULT 'orders-system-kafka'
)


DROP TABLE IF EXISTS dds.s_user_names ;

CREATE TABLE dds.s_user_names (
	h_user_pk uuid NOT NULL references dds.h_user(h_user_pk),
	username varchar(255) NOT NULL ,
	userlogin varchar(255) NOT NULL ,
	load_dt timestamp NOT NULL,
	load_src varchar(255) NOT null DEFAULT 'orders-system-kafka',
	hk_user_names_hashdiff uuid unique NOT NULL,
	PRIMARY KEY (h_user_pk, load_dt)
)


DROP TABLE IF EXISTS dds.s_product_names ;

CREATE TABLE dds.s_product_names (
	h_product_pk uuid NOT NULL references dds.h_product(h_product_pk),
	name varchar(255) NOT NULL ,
	load_dt timestamp NOT NULL,
	load_src varchar(255) NOT null DEFAULT 'orders-system-kafka',
	hk_product_names_hashdiff uuid unique NOT NULL,
	PRIMARY KEY (h_product_pk, load_dt)
)


DROP TABLE IF EXISTS dds.s_restaurant_names ;

CREATE TABLE dds.s_restaurant_names (
	h_restaurant_pk uuid NOT NULL references dds.h_restaurant(h_restaurant_pk),
	"name" varchar(255) NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar(255) NOT null DEFAULT 'orders-system-kafka',
	hk_restaurant_names_hashdiff uuid unique NOT NULL,
	PRIMARY KEY (h_restaurant_pk, load_dt)
)


DROP TABLE IF EXISTS dds.s_order_cost ;

CREATE TABLE dds.s_order_cost (
	h_order_pk uuid NOT NULL references dds.h_order(h_order_pk),
	cost decimal(19, 5) NOT NULL,
	payment decimal(19, 5) NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar(255) NOT null DEFAULT 'orders-system-kafka',
	hk_order_cost_hashdiff uuid unique NOT NULL,
	PRIMARY KEY (h_order_pk, load_dt)
)


DROP TABLE IF EXISTS dds.s_order_status ;

CREATE TABLE dds.s_order_status (
	h_order_pk uuid NOT NULL references dds.h_order(h_order_pk),
	status varchar(255) NOT NULL ,
	load_dt timestamp NOT NULL,
	load_src varchar(255) NOT null DEFAULT 'orders-system-kafka',
	hk_order_status_hashdiff uuid unique NOT NULL,
	PRIMARY KEY (h_order_pk, load_dt)
)
