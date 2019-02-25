from geopy.geocoders import Nominatim

geolocator = Nominatim()

jewelery = geolocator.geocode("Jewelry Shops in California", exactly_one=False)
