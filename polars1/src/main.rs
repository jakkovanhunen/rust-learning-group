/****************************************
 * cargo run --bin polars1
 ****************************************/
#![allow(dead_code, unused_variables, unused_imports)]

use std::io::Cursor;

use polars::prelude::*;

const INDUSTRY: &str = "Industry";
const NAME: &str = "Name";
const NUM_EMPLOYEES: &str = "NumEmployees";
const WEBSITE: &str = "Website";

fn main() -> Result<(), PolarsError> {
    // ////////////////////////////////////////////////
    // @TODO 1
    // Load the organizations-100.csv file into
    // a Polars DataFrame
    // ////////////////////////////////////////////////
    let df = CsvReader::new(Cursor::new(include_str!("data/organizations-100.csv")))
        .has_header(true)
        .finish()?;
    assert_eq!(df.shape(), (100, 9));

    // ////////////////////////////////////////////////
    // @TODO 2
    // Print the data types (dtypes) and shape of the DF
    // ////////////////////////////////////////////////
    println!("Shape: {:?}", df.shape());
    println!("Data Types: {:?}", df.dtypes());

    println!("Schema:");
    for (name, dt) in df.schema().iter() {
        println!("- {name}: {dt}");
    }

    // ////////////////////////////////////////////////
    // @TODO 3
    // The "Industry" column has some empty (NULL) values
    // how many NULL values are there?
    // ////////////////////////////////////////////////
    let null_count = df.column(INDUSTRY)?.null_count();
    println!("Null values in `Industry`: {}", null_count);

    // ////////////////////////////////////////////////
    // @TODO 4
    // Fill Industry NULL values with "Unknown"
    // ////////////////////////////////////////////////
    let df = df
        .lazy()
        // Fill null values in industry with `Unknown`
        .with_column(col(INDUSTRY).fill_null(lit("Unknown")))
        .collect()?;
    assert_eq!(df.column(INDUSTRY)?.null_count(), 0);

    // ////////////////////////////////////////////////
    // @TODO 5
    // Sort the DataFrame by name ASC and show the first 10
    // items of just the "Name" series
    // ////////////////////////////////////////////////
    let sorted_df = df
        .clone()
        .lazy()
        // Sort by name in ascending order
        .select([col(NAME).sort(SortOptions::new().with_order_descending(false))])
        // Get the first 10 rows
        .limit(10)
        .collect()?;
    println!("{:?}", sorted_df);

    // ////////////////////////////////////////////////
    // @TODO 6
    // Show the Industry with the most number of employees
    // ////////////////////////////////////////////////
    let industry_count_df = df
        .clone()
        .lazy()
        // Group by industry and sum the number of employees
        .group_by([col(INDUSTRY)])
        .agg([col(NUM_EMPLOYEES).sum()])
        // Sort by number of employees in descending order
        .sort(
            [NUM_EMPLOYEES],
            SortMultipleOptions::new().with_order_descending(true),
        )
        // Get the first row
        .first()
        .collect()?;
    println!("{:?}", industry_count_df);

    // get the first row
    let row = industry_count_df.get(0).unwrap();
    assert_eq!(row[0], AnyValue::String("Plastics"));
    assert_eq!(row[1], AnyValue::Int64(25894));

    // ////////////////////////////////////////////////
    // @TODO 7
    // Count the total number of employees in organizations
    // under the "Legal Services" industry category
    // ////////////////////////////////////////////////
    let legal_services_emp_count_df = df
        .clone()
        .lazy()
        // Get rows where industry is `Legal Services`
        .filter(col(INDUSTRY).eq(lit("Legal Services")))
        // Group by industry and sum the number of employees
        .group_by([col(INDUSTRY)])
        .agg([col(NUM_EMPLOYEES).sum()])
        .collect()?;
    println!("{:?}", legal_services_emp_count_df);

    // get the first row
    let row = legal_services_emp_count_df.get(0).unwrap();
    assert_eq!(row[1], AnyValue::Int64(8360));

    // ////////////////////////////////////////////////
    // @TODO 8
    // THIS ONE IS CHALLENGING
    // Count the number of organizations where
    // the "Website" series value starts with "https"
    // [hint]: there is a "starts_with" function in one of the features
    // ////////////////////////////////////////////////
    let https_df = df
        .lazy()
        // Get rows where website start with `https`
        .filter(col(WEBSITE).str().starts_with(lit("https")))
        // Get the website column
        .select([col(WEBSITE).alias("Https")])
        // Creates single row with count of rows
        .count()
        .collect()?;
    println!("{:?}", https_df);

    Ok(())
}
