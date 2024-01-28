# List of European countries with their ISO 2-letter codes and capital cities
# The timezone is set to "Europe/Brussels" for all as per the user's request
european_countries = [
    ("Austria", "AT", "Vienna", "Europe/Brussels"),
    ("Belgium", "BE", "Brussels", "Europe/Brussels"),
    ("Bosnia and Herzegovina", "BA", "Sarajevo", "Europe/Belgrade"), #no prices
    ("Bulgaria", "BG", "Sofia", "Europe/Sofia"),
    ("Croatia", "HR", "Zagreb", "Europe/Belgrade"),
    ("Czech Republic", "CZ", "Prague", "Europe/Prague"),
    ("Denmark", "DK", "Copenhagen", "Europe/Copenhagen"), #no prices
    ("Estonia", "EE", "Tallinn", "Europe/Tallinn"),
    ("Finland", "FI", "Helsinki", "Europe/Helsinki"),
    ("France", "FR", "Paris", "Europe/Paris"),
    ("Germany", "DE", "Berlin", "Europe/Berlin"), #no prices
    ("Greece", "GR", "Athens", "Europe/Athens"),
    ("Hungary", "HU", "Budapest", "Europe/Budapest"),
    ("Ireland", "IE", "Dublin", "Europe/Dublin"), #no prices
    ("Italy", "IT", "Rome", "Europe/Rome"), #no prices
    ("Kosovo", "XK", "Pristina", "Europe/Belgrade"), #no prices
    ("Latvia", "LV", "Riga", "Europe/Riga"),
    ("Lithuania", "LT", "Vilnius", "Europe/Vilnius"),
    ("Luxembourg", "LU", "Luxembourg", "Europe/Luxembourg"), #no prices
    ("Moldova", "MD", "Chisinau", "Europe/Chisinau"), #no prices
    ("Montenegro", "ME", "Podgorica", "Europe/Belgrade"),
    ("Netherlands", "NL", "Amsterdam", "Europe/Amsterdam"),
    ("North Macedonia", "MK", "Skopje", "Europe/Belgrade"),
    ("Norway", "NO", "Oslo", "Europe/Oslo"), #no prices
    ("Poland", "PL", "Warsaw", "Europe/Warsaw"),
    ("Portugal", "PT", "Lisbon", "Europe/Lisbon"),
    ("Romania", "RO", "Bucharest", "Europe/Bucharest"),
    ("Serbia", "RS", "Belgrade", "Europe/Belgrade"),
    ("Slovakia", "SK", "Bratislava", "Europe/Bratislava"),
    ("Slovenia", "SI", "Ljubljana", "Europe/Ljubljana"),
    ("Spain", "ES", "Madrid", "Europe/Madrid"),
    ("Sweden", "SE", "Stockholm", "Europe/Stockholm"), #no prices
    ("Switzerland", "CH", "Bern", "Europe/Zurich"),
]

# Base cronjob line with placeholders
base_cronjob = "*/15 * * * * /root/Stromzeiten_datacollector/venv/bin/python /root/Stromzeiten_datacollector/data_loader.py {iso_code} {country_name} {capital_city} Europe/Brussels > /root/tmp/{iso_code}.log 2>&1"

# Generate cronjob lines for each country
cronjob_lines = [base_cronjob.format(iso_code=country[1], country_name=country[0], capital_city=country[2]) for country in european_countries]
cronjob_lines[:5]  # Show the first five as an example