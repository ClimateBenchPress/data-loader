# ClimateBenchPress Data Loader

This repository contains the code to download the datasets for the ClimateBenchPress compression benchmark. 

## Getting Started

This project uses the uv package manager to handle dependencies. If you don't already have it installed follow the instructions at [https://docs.astral.sh/uv/getting-started/installation/](https://docs.astral.sh/uv/getting-started/installation/). 

Next, clone this repository and within the project directory install all the necessary dependencies with:
```bash
uv sync
uv pip install -e ".[data]"
```

## Downloading the Data

To download all the data used for the benchmark run the following commands:
```bash
uv run python -m climatebenchpress.data_loader.datasets.esa_biomass_cci
uv run python -m climatebenchpress.data_loader.datasets.cams
uv run python -m climatebenchpress.data_loader.datasets.era5
uv run python -m climatebenchpress.data_loader.datasets.nextgems
uv run python -m climatebenchpress.data_loader.datasets.cmip6.access_ta
uv run python -m climatebenchpress.data_loader.datasets.cmip6.access_tos
```
This will download the data into a sub-directory named `datasets` within this repository. If you want to store the data in a different directory you can use the `--basepath=${path/to/dir}` command line argument for the scripts which will store the data at `${path/to/dir}/datasets` instead.

## Funding 

ClimateBenchPress has been developed as part of [Embed2Scale](https://embed2scale.eu/) and [ESiWACE3](https://www.esiwace.eu/).

Funded by the European Union. This work has received funding from the European High Performance Computing Joint Undertaking (JU) under grant agreement No 101093054 and EU’s Horizon Europe program under grant agreement number 101131841. This work also received funding from [UK Research and Innovation (UKRI)](https://www.ukri.org/).