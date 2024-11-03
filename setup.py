import os

packages = ["matplotlib", "pandas", "folium", "requests"]

for st_ in packages:
    os.system("pip install " + st_)

os.system("python3 -m pip install -U --pre shapely")
