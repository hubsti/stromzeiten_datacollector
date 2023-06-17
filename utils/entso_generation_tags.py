TAGS_RENEW: dict[str, str] = {
    "Biomass": "Biomass",
    "Geothermal": "Geothermal",
    "Hydro_storage": "Hydro Pumped Storage",
    "Hydro": "Hydro Run-of-river and poundage",
    "Hydro_res": "Hydro Water Reservoir",
    "Other_renew": "Other renewable",
    "Solar": "Solar",
    "Wind_off": "Wind Offshore",
    "Wind_on": "Wind Onshore",
}

TAGS_NON_RENEW: dict[str, str] = {
    "Lignite": "Fossil Brown coal/Lignite",
    "Gas": "Fossil Gas",
    "Coal": "Fossil Hard coal",
    "Oil": "Fossil Oil",
    "Nuclear": "Nuclear",
    "Other": "Other",
    "Waste": "Waste",
}
ALL_TAGS: dict[str, str] = TAGS_NON_RENEW | TAGS_RENEW