import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

def insert_dataframe(df, engine):
    with engine.connect() as connection:
        for index, row in df.iterrows():
            columns = ', '.join([f'"{col}"' for col in row.index])
            placeholders = ', '.join([':{}'.format(col) for col in row.index])
            values = row.to_dict()
            updates = ', '.join([f'"{column}" = :{column}' for column in row.index])
            print(columns, values)
            insert_query = text(f"""
              SELECT column_name 
FROM information_schema.columns 
WHERE table_name = 'generation';
            """)
            print(insert_query)
            result = connection.execute(insert_query)
            print(result)
            print(connection.execute(insert_query))


def update_dataframe(df, engine, country_code, table_name, index_label='index'):
    # Create a Session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Get the first and last timestamp of the DataFrame
    first_timestamp = df.index.min()
    last_timestamp = df.index.max()

    # Select rows from the database between the first and last timestamp
    query = text(f"""
        SELECT * 
        FROM {table_name} 
        WHERE {index_label} BETWEEN :first_timestamp AND :last_timestamp AND country_code = :country_code
    """)
    result = session.execute(query, params={'first_timestamp': first_timestamp, 'last_timestamp': last_timestamp, 'country_code': country_code})
    db_df = pd.DataFrame(result.fetchall(), columns=result.keys())
    db_df = db_df.set_index(index_label)
    db_df.index.name = None
    db_df.columns.name = None
    print('---------------------db df---------------------')
    print(db_df)
    # Perform an outer join of df and db_df on index
    print('---------------------oryginal---------------------')
    print(df)

    # Perform an outer join of df and db_df on index
    merged_df = df.merge(db_df, how='outer', indicator=True, left_index=True, right_index=True)
    #merged_df = merged_df.set_index('index')
        # Drop columns that end with _y
    merged_df = merged_df[merged_df.columns.drop(list(merged_df.filter(regex='_y$')))]

    # Rename columns that end with _x by removing _x
    merged_df.columns = [col.replace('_x', '') if '_x' in col else col for col in merged_df.columns]
    new_df = merged_df[merged_df['_merge'] == 'left_only']
    new_df = new_df.drop(columns='_merge')
    if table_name == 'forecast_data':
        new_df.index.name = "time"
    print(merged_df)
    # Take only the rows that exist in df and don't exist in db_df


    print('---------------------after merge---------------------')
    print(new_df)
    # Write these rows to the database
    new_df.to_sql(name=table_name, con=engine, if_exists='append')