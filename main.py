import requests
import duckdb
import dlt


# -----------------------------
# DLT Source
# -----------------------------
@dlt.source
def get_rides():

    @dlt.resource(name="rides", write_disposition="replace")
    def rides_resource():

        page = 1

        while True:
            url = f"https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api?page={page}"

            print(f"Page {page} loaded.")

            data = requests.get(url).json()

            if len(data) == 0:
                break

            yield data
            page += 1

    return rides_resource


# -----------------------------
# Create Pipeline
# -----------------------------
pipeline = dlt.pipeline(
    pipeline_name="ny_taxi",
    destination="duckdb",
    dataset_name="rides",
    progress="log"
)


# -----------------------------
# Run Pipeline
# -----------------------------
def run_pipeline():
    pipeline.run(get_rides())


# -----------------------------
# Query DuckDB
# -----------------------------
def run_queries():

    conn = duckdb.connect("ny_taxi.duckdb")

    rides_table = "ny_taxi.rides.rides"

    print("\nSample data")
    print(conn.sql(f"SELECT * FROM {rides_table} LIMIT 5").df())

    print("\nTrip date range")
    print(
        conn.sql(
            f"""
            SELECT
                max(trip_pickup_date_time) as start_date,
                max(trip_dropoff_date_time) as end_date
            FROM {rides_table}
            """
        ).df()
    )

    print("\nCredit card percentage")
    print(
        conn.sql(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE payment_type = 'Credit') AS Credit,
                COUNT(*) AS Total,
                Credit * 1.0 / Total AS Percentage
            FROM {rides_table}
            """
        ).df()
    )

    print("\nTotal tips")
    print(
        conn.sql(
            f"""
            SELECT SUM(tip_amt) as total_tips
            FROM {rides_table}
            """
        ).df()
    )


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":

    run_pipeline()

    run_queries()