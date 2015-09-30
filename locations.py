import json

class Location:
    def __init__(self, crs, tiploc, name, toc):
        self.crs = crs
        self.tiploc = tiploc
        self.name = name
        self.toc = toc

    def __str__(self):
        return "{} - {} - {} - {}".format(self.crs, self.tiploc, self.toc, self.name)

    def __repr__(self):
        return self.__str__()

class LocationMapper:
    def __init__(self, locations_file_path):
        with open(locations_file_path) as f:
            locations = json.loads(f.read())
            self.location_map = {}
            for l in locations["locations"]:
                if "_tpl" in l and "_crs" in l and "_toc" in l and "_locname" in l:
                    nl = Location(l["_crs"], l["_tpl"], l["_locname"], l["_toc"])
                    self.location_map[nl.tiploc] = nl

    def get_crs(self, tiploc):
        return self.location_map[tiploc].crs

    def get_name(self, tiploc):
        return self.location_map[tiploc].name

class CrsMapper:
    def __init__(self, locations_file_path):
        with open(locations_file_path) as f:
            locations = json.loads(f.read())
            self.locations_map = {}
            for l in locations["locations"]:
                if l["_crs"] in self.locations_map:
                    continue
                self.locations_map[l["_crs"]] = {"crs": l["_crs"], "lat": l["_lat"], "lon": l["_lon"], "name": l["_locname"], "toc": l["_toc"] }

    def name(self, crs):
        return self.locations_map[crs]["name"]

    def lat(self, crs):
        return self.locations_map[crs]["lat"]

    def lon(self, crs):
        return self.locations_map[crs]["lon"]

    def toc(self, crs):
        return self.locations_map[crs]["toc"]

if __name__ == "__main__":
    lm = LocationMapper("locations.json")
    print("PLYMTH -> {} [{}].".format(lm.get_crs("PLYMTH"), lm.get_name("PLYMTH")))


