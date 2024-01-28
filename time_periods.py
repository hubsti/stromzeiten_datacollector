from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import sessionmaker

from sqlalchemy import create_engine, text


def create_time_periods(country_code):
    # Create a SQLAlchemy engine
    alchemyEngine = create_engine(
        "postgresql+psycopg2://postgres:<3Fuksik69@127.0.0.1", pool_recycle=3600
    )

    # Query the data for the current day and two days ahead for a specific country code
    query = f"""
    SELECT time, "Cei_prediction"
    FROM forecast_data
    WHERE country_code = '{country_code}'
    AND time >= CURRENT_DATE
    AND time < CURRENT_DATE + INTERVAL '3 days'
    ORDER BY time
    """
    df = pd.read_sql_query(query, alchemyEngine)

    # Group the data by day and hour
    df["date"] = df["time"].dt.date
    df["hour"] = df["time"].dt.hour
    grouped = df.groupby(["date", "hour"])["Cei_prediction"].mean()

    # Create the dictionary
    result = []
    query = f"""
    SELECT average_cei
    FROM average_cei
    WHERE country_code = '{country_code}'
    """
    average_CEI = pd.read_sql_query(query, alchemyEngine).iloc[0, 0]

    print("-----------------average_CEI------------------")
    print(average_CEI)
    for (date, hour), averageIntensity in grouped.items():
        start = datetime(date.year, date.month, date.day, hour).astimezone(timezone.utc)
        end = (start + timedelta(hours=1)).astimezone(timezone.utc)
        if averageIntensity < 1 * average_CEI:
            if (
                result
                and result[-1]["end"] == start.isoformat(timespec="milliseconds")
                and result[-1]["date"] == date
            ):
                # If the last period ends where this one starts, extend the last period
                result[-1]["end"] = end.isoformat(timespec="milliseconds")
                # Calculate the new average intensity
                duration = (end - start).total_seconds() / 3600
                result[-1]["averageIntensity"] = (
                    result[-1]["averageIntensity"] * (duration - 1) + averageIntensity
                ) / duration
            else:
                # Otherwise, create a new period
                result.append(
                    {
                        "date": date,
                        "start": start.isoformat(timespec="milliseconds"),
                        "end": end.isoformat(timespec="milliseconds"),
                        "averageIntensity": averageIntensity,
                        "country_code": country_code,
                    }
                )
        else:
            # If there are no periods for a specific date, add a placeholder period
            result.append(
                {
                    "date": date,
                    "start": "nothing",
                    "end": "nothing",
                    "averageIntensity": 0,
                    "country_code": country_code,
                }
            )

    # Convert the result to a DataFrame and save it to the database
    df_result = pd.DataFrame(result)
    df_result = df_result[
        ~((df_result["start"] == "nothing") & (df_result["end"] == "nothing"))
        | (~df_result.duplicated(["date"]))
    ]


    # Sort by date and averageIntensity
    df_result = df_result.sort_values(["date", "averageIntensity"])

    # Group by date and take the periods with the lowest carbon intensity
    df_result = (
        df_result.groupby("date")
        .apply(lambda x: x.nsmallest(2, "averageIntensity"))
        .reset_index(drop=True)
    )
    df_result.set_index("date", inplace=True)
    # Save the filtered results to the database
    #
    # df_result.to_sql("time_periods", con=alchemyEngine, if_exists="replace")
    update_dataframe_ts(
        df_result, alchemyEngine, country_code, "time_periods", index_label="date"
    )
    print(df_result)


def update_dataframe_ts(df, engine, country_code, table_name, index_label="index"):
    # Create a Session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Get the first and last timestamp of the DataFrame
    first_timestamp = df.index.min()
    last_timestamp = df.index.max()

    # Select rows from the database between the first and last timestamp
    query = text(
        f"""
        SELECT * 
        FROM {table_name} 
        WHERE {index_label} BETWEEN :first_timestamp AND :last_timestamp AND country_code = :country_code
    """
    )
    result = session.execute(
        query,
        params={
            "first_timestamp": first_timestamp,
            "last_timestamp": last_timestamp,
            "country_code": country_code,
        },
    )
    db_df = pd.DataFrame(result.fetchall(), columns=result.keys())
    db_df = db_df.set_index(index_label)
    db_df.index.name = None
    db_df.columns.name = None

    # Perform an outer join of df and db_df on index
    merged_df = df.merge(
        db_df, how="outer", indicator=True, left_index=True, right_index=True
    )

    # merged_df = merged_df.set_index('index')
    # Drop columns that end with _y
    merged_df = merged_df[merged_df.columns.drop(list(merged_df.filter(regex="_y$")))]

    # Rename columns that end with _x by removing _x
    merged_df.columns = [
        col.replace("_x", "") if "_x" in col else col for col in merged_df.columns
    ]
   
    new_df = merged_df[merged_df["_merge"] == "left_only"]
    new_df = new_df.drop(columns="_merge")
    if table_name == "forecast_data":
        new_df.index.name = "time"
    # Take only the rows that exist in df and don't exist in db_df
    # Write these rows to the database
    
    if table_name == "time_periods":
        new_df.index.name = "date"
    new_df.to_sql(name=table_name, con=engine, if_exists="append")
    print(new_df)

create_time_periods("CZ")
