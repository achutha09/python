
IF NOT EXISTS (SELECT schema_id FROM sys.schemas WHERE name = 'ss')
    EXEC('CREATE SCHEMA ss;');
    
Drop table IF Exists ss.FACT_OrderDetails;
Drop table IF Exists ss.DIM_order_employee;
Drop table IF Exists ss.DIM_order_shipper

Drop table IF Exists ss.DIM_product;
Drop table IF Exists ss.DIM_customer;
Drop table IF Exists ss.DIM_product_category;
Drop table IF Exists ss.DIM_product_supplier;
Drop sequence IF Exists ss.primaryKeyForOrderDetails;



CREATE SEQUENCE ss.primaryKeyForOrderDetails
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 100000
    START WITH 1;


CREATE TABLE ss.FACT_OrderDetails (
    id BIGINT PRIMARY KEY DEFAULT NEXT VALUE FOR ss.primaryKeyForOrderDetails,
    OrderID int,
    price DECIMAL(10, 2),
    quantity int,
    Discount DECIMAL(4, 2),
    order_date DATE,
    product_Id int,
    customer_Id varchar(50),
    shipper_Id int,
    employee_Id int,
    supplier_Id int,
    category_Id int
   );


CREATE TABLE ss.DIM_product (
    product_Id int PRIMARY key,
    product_name VARCHAR(255),
    quantity_per_unit VARCHAR(50),
    unit_price DECIMAL(10, 2)
);

CREATE TABLE ss.DIM_product_supplier (
    supplier_Id int PRIMARY key,
    company_name VARCHAR(255),
    region VARCHAR(50),
    country VARCHAR(50),
    eff_start_date DATE,
    eff_end_date DATE
);

-- Create DIM_Category Table
CREATE TABLE ss.DIM_product_category (
    category_Id int PRIMARY key,
    category_name VARCHAR(50)
);


CREATE TABLE ss.DIM_customer (
    customer_Id Varchar(50) PRIMARY key,
    company_name VARCHAR(255),
    region VARCHAR(50),
    country VARCHAR(50)
);

-- Create DIM_Shipper Table
CREATE TABLE ss.DIM_order_shipper (
    shipper_Id int PRIMARY key,
    company_name VARCHAR(255)
);

-- Create DIM_Employee Table
CREATE TABLE ss.DIM_order_employee (
    employee_Id int PRIMARY key,
    employee_name VARCHAR(255),
    title VARCHAR(50),
    country VARCHAR(50)
);