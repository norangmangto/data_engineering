from wetterdienst.provider.dwd.observation import DwdObservationRequest
from wetterdienst import Settings

try:
    print("Testing with ('daily', 'climate_summary')...")
    request = DwdObservationRequest(
        parameters=[("daily", "climate_summary")],
        periods=["historical"],
        settings=Settings(ts_shape="long")
    )
    print("Success with climate_summary!")
    stations = request.filter_by_rank(latlon=(52.52, 13.40), rank=5)
    print(stations.values.df.head())
except Exception as e:
    print(f"Failed with climate_summary: {e}")
