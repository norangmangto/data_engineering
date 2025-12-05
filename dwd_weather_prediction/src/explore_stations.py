from wetterdienst.provider.dwd.observation import DwdObservationRequest
from wetterdienst import Settings

def list_stations(parameter="temperature_air", resolution="daily", period="historical"):
    """
    List available DWD stations for a given parameter.
    """
    settings = Settings(ts_shape="long")

    request = DwdObservationRequest(
        parameters=[("daily", "climate_summary")],
        periods=[period],
        settings=settings
    )

    stations = request.filter_by_rank(latlon=(51.2277, 6.7735), rank=5) # Düsseldorf coordinates

    # Check if we can get the dataframe
    try:
        df = stations.df
    except:
        df = stations.to_pandas()

    print("Columns:", df.columns)
    print(df[["station_id", "name", "state", "start_date", "end_date", "distance"]])

if __name__ == "__main__":
    print("Finding stations near Berlin for Temperature (Daily)...")
    list_stations()
