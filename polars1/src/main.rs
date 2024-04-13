/****************************************
 * cargo run --bin polars1
 ****************************************/
#![allow(dead_code, unused_variables, unused_imports)]

use std::io::Cursor;

use polars::prelude::*;

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
    let null_count = df.column("Industry")?.null_count();
    println!("Null values in `Industry`: {}", null_count);

    // ////////////////////////////////////////////////
    // @TODO 4
    // Fill Industry NULL values with "Unknown"
    // ////////////////////////////////////////////////
    let df = df
        .lazy()
        .with_column(col("Industry").fill_null(lit("Unknown")))
        .collect()?;
    assert_eq!(df.column("Industry")?.null_count(), 0);

    // ////////////////////////////////////////////////
    // @TODO 5
    // Sort the DataFrame by name ASC and show the first 10
    // items of just the "Name" series
    // ////////////////////////////////////////////////
    let sorted_df = df
        .clone()
        .lazy()
        .select([col("Name").sort(false)])
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
        .group_by([col("Industry")])
        .agg([col("NumEmployees").sum()])
        .sort(
            "NumEmployees",
            SortOptions {
                descending: true,
                ..Default::default()
            },
        )
        .limit(1)
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
        .filter(col("Industry").eq(lit("Legal Services")))
        .group_by([col("Industry")])
        .agg([col("NumEmployees").sum()])
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
        .clone()
        .lazy()
        .filter(col("Website").str().starts_with(lit("https")))
        .collect()?;
    println!("Answer 8: {:?}", https_df.height());

    Ok(())
}
