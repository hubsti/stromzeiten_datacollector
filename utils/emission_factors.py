#emission factors in gCO₂eq/kWh

CO2_FACTORS: dict[str, int] = {
    "Biomass": 230,
    "Lignite": 1104,
    "Gas": 515,
    "Coal": 1104,
    "Oil": 1125,
    "Geothermal": 38,
    "Hydro_storage": 859,
    "Hydro": 11,
    "Hydro_res": 11,
    "Nuclear": 5,
    "Other": 700,
    "Other_renew": 300,
    "Solar": 35,
    "Waste": 700,
    "Wind_off": 13,
    "Wind_on": 13,
}