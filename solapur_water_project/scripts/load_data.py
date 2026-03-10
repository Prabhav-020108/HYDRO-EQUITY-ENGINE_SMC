import numpy as np
import json
import pandas as pd

with open("Data/pipeline.geojson") as f:
    Data = json.load(f)

pipes = []

for feature in Data["features"]:

    props = feature["properties"]
    coords = feature["geometry"]["coordinates"]

    pipes.append({
        "material": props["Material"],
        "diameter": props["Diameter(m"],
        "length": props["Length(m)"],
        "zone": props["Water Zone"],
        "coords": coords
    })

pipes_df = pd.DataFrame(pipes)

print(pipes_df.head())

nodes = []

for pipe in pipes:

    coords = pipe["coords"][0]

    start = coords[0]
    end = coords[-1]

    nodes.append(start)
    nodes.append(end)

nodes_df = pd.DataFrame(nodes, columns=["lon","lat"])

nodes_df = nodes_df.drop_duplicates()

nodes_df = nodes_df.drop_duplicates()
np.random.seed(42)

nodes_df["elevation"] = np.random.uniform(440,470,len(nodes_df))

print(nodes_df.head())

nodes_df.to_csv("Data/nodes_with_elevation.csv", index=False)