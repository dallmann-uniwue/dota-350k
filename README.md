# Dota-350k
Dota-350k is a dataset comprised of all ranked matches played in Dota 2 on the 16th of April 2020 (Patch Version: 7.25c).

You can find out more about the dataset in our paper:

A. Dallmann, J. Kohlmann, D. Zoller and A. Hotho, "[Sequential Item Recommendation in the MOBA Game Dota 2](https://ieeexplore.ieee.org/document/9679858)," 2021 International Conference on Data Mining Workshops (ICDMW), 2021, pp. 10-17, doi: 10.1109/ICDMW53433.2021.00009.

If you're using the dataset please cite the above paper.

## Download
### Raw Datasets
These packages contain the unprocessed raw datasets:
* [Opendota RAW](https://professor-x.de/datasets/opendota-raw.tar.xz)
* [Dota-350k RAW](https://professor-x.de/datasets/dota-350k-raw.tar.xz)

### Processed Datasets
These packages contain the processed datasets and splits:
* [Opendota](https://professor-x.de/datasets/opendota-v3.tar.xz)
* [Dota-350k](https://professor-x.de/datasets/dota-350k-v3.tar.xz)

## Dataset Processing
The repository contains code for processing the raw [Dota-350k](https://professor-x.de/papers/dota-350k) dataset. 
It also contains a script to convert the [Opendota dataset](https://www.kaggle.com/datasets/devinanzelmo/dota-2-matches) from Kaggle to enable processing with the same scripts.

### Setup
This project uses [Poetry](https://python-poetry.org) to manage the build process. 

Executing:

```bash
poetry shell
```
prompts poetry to create and activate a virtual environment with the necessary dependencies that you can use to execute
the scripts.

### Convert OpenDota
Download and unpack the [Opendota dataset](https://www.kaggle.com/datasets/devinanzelmo/dota-2-matches) from Kaggle. 
Then execute:

```bash
python -m dataset_processing.opendota.convert <unpacked-opendota-directory> <dataset-file> --file-type csv
```

### Process OpenDota / Dota-350k
Processing is done in multiple steps. In order to speed up the computation, we use Spark to process the data and `Parquet` as the input file format. If you only have the CSV file, you need to convert it to parquet first:

```bash
python -m dataset_processing.dota.prepare_dataset to-parquet <input-file-csv> <output-directory-parquet>
```

Then you can start processing the datasets:

```bash
RAW_PATH=dota-350k-raw   # /path/that/contains/raw/dataset
DATASET_PATH=dota-350k   # /path/where/data/is/written

# Dota-350k
python -m  dataset_processing.dota.add_metadata $RAW_PATH/dota.parquet $DATASET_PATH ../resources/roles.json ../resources/heros.json
# Opendota
#python -m  dataset_processing.dota.add_metadata --opendota $RAW_PATH/dota.parquet $DATASET_PATH ../resources/roles.json ../resources/heros.json

python -m  dataset_processing.dota.prepare_dataset process --items-file ../resources/items-7.25c.json  $DATASET_PATH/dota.parquet $DATASET_PATH
python -m  dataset_processing.dota.prepare_dataset split --training-ratio 0.94 --validation-ratio 0.01 --test-ratio 0.05  $DATASET_PATH/dota-processed.parquet $DATASET_PATH

```