# inaturalist-cc0-photos
This script is used to create a list of iNaturalist research-grade observations of species with a documented English 
vernacular (common) name and a CC0-licensed photo. 

The source data consists of DWCA taxonomy and observation exports provided by iNaturalist.
* Taxonomy: https://www.inaturalist.org/taxa/inaturalist-taxonomy.dwca.zip
* Observations: http://www.inaturalist.org/observations/gbif-observations-dwca.zip

Extract the above two resources and place them in the `data` directory (note: these directories unzip, at the 
time of writing, to a combined size of about 100GB). Then, run `python3 main.py`.

The script will create three CSV files in the `out` directory:
* `common_name_observations.csv` - scientific and vernacular names for observed species, and the observation id.
* `cc0_photos.csv` - all CC0-licensed observation photos from the data export and the corresponding scientific name for 
  the species depicted.
* `cc0_photos_with_common_name.csv` - the combined vernacular names, scientific names, and photos.

This is primarily for personal use, but could be a good starting point for those who want to work with 
iNaturalist data.
