import csv
import datetime as dt
import logging
import typing as t
from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4

import requests
# import kartverket
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryCreateEmptyDatasetOperator
from google.cloud import bigquery

import config
from config import DEFAULT_DAG_ARGS

logger = logging.getLogger(__name__)


@dataclass
class SchemaField:
    api_name: str
    name: str
    type_: str
    mode: str
    description: str


SCHEMA_FIELDS = [
    SchemaField(
        api_name='matrikkelnummer',
        name='matrikkelnummer',
        type_='STRING',
        mode='NULLABLE',
        description='Det sammensatte matrikkelnummeret (Kommunenr-Gårdsnr/Bruksnr/Festenr/Seksjonsnr).'
    ),
    SchemaField(
        api_name='bruksnummer',
        name='bruksnummer',
        type_='INTEGER',
        mode='NULLABLE',
        description='Del av et matrikkelnummer.'
    ),
    SchemaField(
        api_name='festenummer',
        name='festenummer',
        type_='INTEGER',
        mode='NULLABLE',
        description='Del av et matrikkelnummer.'
    ),
    SchemaField(
        api_name='gardsnummer',
        name='gardsnummer',
        type_='INTEGER',
        mode='NULLABLE',
        description='Del av et matrikkelnummer.'
    ),
    SchemaField(
        api_name='hovedområde',
        name='hovedomrade',
        type_='BOOLEAN',
        mode='NULLABLE',
        description='Angir om området er teigens eller anleggsprojeksjonens hovedteig/hovedflate.'
    ),
    SchemaField(
        api_name='kommunenummer',
        name='kommunenummer',
        type_='STRING',
        mode='NULLABLE',
        description='Kommunenummer bestående av fire tegn med ledende 0 om nødvendig.'
    ),
    SchemaField(
        api_name='lokalid',
        name='lokalid',
        type_='INTEGER',
        mode='NULLABLE',
        description='Lokal identifikator, tildelt av dataleverandør/dataforvalter (her matrikkelsystemet, Kartverket).'
    ),
    SchemaField(
        api_name='matrikkelnummertekst',
        name='matrikkelnummertekst',
        type_='STRING',
        mode='NULLABLE',
        description='Generert tekst ut fra hvilken matrikkelenhet teigen tilhører. Eventuelt flere matrikkelnummere skyldes manglende, uavklarte grenser eller uregistrert jordsameie.'
    ),
    SchemaField(
        api_name='meterFraPunkt',
        name='meterFraPunkt',
        type_='INTEGER',
        mode='NULLABLE',
        description='Distanse i meter til punktet det ble søkt etter.'
    ),
    SchemaField(
        api_name='nøyaktighetsklasseteig',
        name='noyaktighetsklasseteig',
        type_='STRING',
        mode='NULLABLE',
        description='Grov klassifisering (trafikklys) av stedfestingsnøyaktighet. (Grønt = ok, gult = sjekk!, rødt = store mangler)'
    ),
    SchemaField(
        api_name='objekttype',
        name='objekttype',
        type_='STRING',
        mode='NULLABLE',
        description='Stedfesting/geometri hentes fra to objekttyper, teig eller anleggsprojeksjonsflate. Den siste er «fotavtrykk» av volumer som fins over eller under teiger på terrenget'
    ),
    SchemaField(
        api_name='oppdateringsdato',
        name='oppdateringsdato',
        type_='DATETIME',
        mode='NULLABLE',
        description='dato for siste endring på data-objektet i matrikkelsystemet'
    ),
    SchemaField(
        api_name='representasjonspunkt_koordsys',
        name='representasjonspunkt_koordsys',
        type_='INTEGER',
        mode='NULLABLE',
        description=''
    ),
    SchemaField(
        api_name='representasjonspunkt_nord',
        name='representasjonspunkt_nord',
        type_='FLOAT',
        mode='NULLABLE',
        description=''
    ),
    SchemaField(
        api_name='representasjonspunkt_ost',
        name='representasjonspunkt_ost',
        type_='FLOAT',
        mode='NULLABLE',
        description=''
    ),
    SchemaField(
        api_name='seksjonsnummer',
        name='seksjonsnummer',
        type_='INTEGER',
        mode='NULLABLE',
        description='Del av et matrikkelnummer'
    ),
    SchemaField(
        api_name='teigmedflerematrikkelenheter',
        name='teigmedflerematrikkelenheter',
        type_='BOOLEAN',
        mode='NULLABLE',
        description='Teigen mangler indre avgrensing mellom de registrerte matrikkelnummerene'
    ),
    SchemaField(
        api_name='uregistrertjordsameie',
        name='uregistrertjordsameie',
        type_='BOOLEAN',
        mode='NULLABLE',
        description='De registrerte matrikkelnummerene har andel i teigen'
    ),
    SchemaField(
        api_name='',
        name='BatchId',
        type_='STRING',
        mode='REQUIRED',
        description='En unik ID for den aktuelle datainnlastingen'
    ),
    SchemaField(
        api_name='',
        name='LoadTimestamp',
        type_='DATETIME',
        mode='REQUIRED',
        description='Tidspunktet data ble lastet inn'
    ),
]


def get_schema_fields_bq() -> list[dict[str, str]]:
    return [{'name': sf.name, 'type': sf.type_, 'mode': sf.mode, 'description': sf.description} for sf in SCHEMA_FIELDS]


class DownloadException(Exception):
    pass


@task
def download_punkt_data(
    nord: float,
    ost: float,
    koordsys: int = 4258,
    radius: int = 1000,
    treffPerSide: int = 500,
    base_url: str = 'https://api.kartverket.no/eiendom/v1/punkt',
) -> list[dict[str, t.Any]]:
    logger.info(
        f"Starting download of punkt data: nord={nord}, ost={ost}, "
        f"koordsys={koordsys}, radius={radius}, treffPerSide={treffPerSide}"
    )

    res = requests.get(
        url=base_url,
        params={"nord": nord, "ost": ost, "koordsys": koordsys, "radius": radius, "side": 1, "treffPerSide": treffPerSide},
    )

    if res.status_code != 200:
        logger.error(f"Couldn't download data. Status code: {res.status_code}")
        raise DownloadException(f"Couldn't download data. Status code: {res.status_code}")

    data = res.json()
    eiendom = data['eiendom']
    metadata = data['metadata']

    logger.info(
        f"Initial download successful. Found {metadata['totaltAntallTreff']} total entries. "
        f"Retrieved {metadata['viserFra']}-{metadata['viserTil']}"
    )

    side = 1
    while metadata['viserTil'] < metadata['totaltAntallTreff']:
        side += 1
        logger.debug(f"Downloading page {side}")

        res = requests.get(
            url=base_url,
            params={"nord": nord, "ost": ost, "koordsys": koordsys, "radius": radius, "side": side, "treffPerSide": treffPerSide},
        )

        if res.status_code != 200:
            logger.error(f"Failed to download page {side}. Status code: {res.status_code}")
            raise DownloadException(f"Failed to download page {side}. Status code: {res.status_code}")

        data = res.json()
        eiendom.extend(data['eiendom'])
        metadata = data['metadata']

        logger.debug(
            f"Downloaded page {side}: entries {metadata['viserFra']}-{metadata['viserTil']} "
            f"of {metadata['totaltAntallTreff']}"
        )

    logger.info(f"Download completed successfully. Retrieved {len(eiendom)} entries.")
    return eiendom


@task
def transform_punkt_data(data: list[dict[str, t.Any]]) -> list[dict[str, t.Any]]:
    logger.info(f"Starting transformation of {len(data)} punkt records to BigQuery CSV format")

    uuid = str(uuid4())
    load_timestamp = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def get_matrikkelnummer(row) -> str | None:
        # Kommunenr-Gårdsnr/Bruksnr/Festenr/Seksjonsnr
        matrikkelnummer = ''
        if row['kommunenummer']:
            matrikkelnummer += row['kommunenummer']
        else:
            return matrikkelnummer

        if row['gardsnummer']:
            matrikkelnummer += f"-{row['gardsnummer']}"
        else:
            return matrikkelnummer

        if row['bruksnummer']:
            matrikkelnummer += f"/{row['bruksnummer']}"
        else:
            return matrikkelnummer

        if row['festenummer']:
            matrikkelnummer += f"/{row['festenummer']}"
        else:
            return matrikkelnummer

        if row['seksjonsnummer']:
            matrikkelnummer += f"/{row['seksjonsnummer']}"
        else:
            return matrikkelnummer

        return matrikkelnummer

    try:
        for row in data:
            row['BatchId'] = uuid
            row['LoadTimestamp'] = load_timestamp

            # Kommunenr-Gårdsnr/Bruksnr/Festenr/Seksjonsnr
            row['matrikkelnummer'] = get_matrikkelnummer(row)

            representasjonspunkt = row['representasjonspunkt']
            row['representasjonspunkt_koordsys'] = representasjonspunkt['koordsys']
            row['representasjonspunkt_nord'] = representasjonspunkt['nord']
            row['representasjonspunkt_ost'] = representasjonspunkt['øst']
            del row['representasjonspunkt']

            row['hovedomrade'] = row['hovedområde']
            del row['hovedområde']

            row['noyaktighetsklasseteig'] = row['nøyaktighetsklasseteig']
            del row['nøyaktighetsklasseteig']

            row['oppdateringsdato'] = dt.datetime.fromisoformat('2024-01-01T00:00:00').strftime('%Y-%m-%d %H:%M:%S')

        logger.debug(f"Transformed {len(data)} rows successfully")

        return data

    except Exception as e:
        logger.error(f"Error transforming punkt data: {str(e)}", exc_info=True)
        raise


@task
def convert_data_to_csv(data: list[dict[str, t.Any]]) -> str:
    try:
        path = str(config.OUTPUT_DIR / f"punkt_{dt.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv")
        logger.info(f"Writing transformed data to CSV file: {path}")

        field_names = [sf.name for sf in SCHEMA_FIELDS]

        with open(path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writeheader()
            for row in data:
                cleaned_row = {k: v for k, v in row.items() if k in field_names}
                writer.writerow(cleaned_row)

        logger.info(f"Successfully wrote {len(data)} rows to {path}")
    except Exception as e:
        logger.error(f"Error transforming punkt data: {str(e)}", exc_info=True)
        raise

    return path


@task
def insert_csv_to_bigquery(project_id: str, dataset_id: str, table_id: str, csv_path: str) -> None:
    logger.info(f"Starting BigQuery load for table {dataset_id}.{table_id} from {csv_path}")

    try:
        client = bigquery.Client()
        logger.debug(f"Created BigQuery client for project {project_id}")

        full_table_id = f"{project_id}.{dataset_id}.{table_id}"
        logger.debug(f"Full table ID: {full_table_id}")

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField(name=sf.name, field_type=sf.type_, mode=sf.mode, description=sf.description)
                for sf in SCHEMA_FIELDS
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        )
        logger.debug("Created job configuration with schema")

        with open(csv_path, 'rb') as fp:
            logger.info("Starting file upload to BigQuery")
            load_job = client.load_table_from_file(
                file_obj=fp,
                destination=full_table_id,
                job_config=job_config,
            )

            logger.info("Waiting for load job to complete...")
            load_job.result()
            logger.info("Load job completed successfully")

        destination_table = client.get_table(full_table_id)
        row_count = destination_table.num_rows
        logger.info(f"Successfully loaded {row_count} rows to {full_table_id}")
        print(f"Loaded {row_count} rows.")

    except FileNotFoundError as e:
        logger.error(f"CSV file not found at {csv_path}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error loading data to BigQuery: {str(e)}")
        raise


@dag(
    dag_id="kartverket_punkt",
    default_args=DEFAULT_DAG_ARGS,
    description="",
    schedule_interval='0 12 * * *',
    start_date=datetime(2024, 12, 10),
    catchup=False,
    tags=["kartverket"],
)
def kartverket_punkt_dag():
    PROJECT_ID = "rikard-test"
    DATASET_ID = 'Kartverket'
    TABLE_ID = 'Punkt'

    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset", dataset_id=DATASET_ID, exists_ok=True)

    downloaded_data = download_punkt_data(nord=60.5, ost=11.12)
    transformed_data = transform_punkt_data(data=downloaded_data)  # type: ignore
    csv_path = convert_data_to_csv(data=transformed_data)  # type: ignore
    final_job = insert_csv_to_bigquery(project_id=PROJECT_ID, dataset_id=DATASET_ID, table_id=TABLE_ID, csv_path=csv_path)  # type: ignore

    create_dataset >> downloaded_data >> transformed_data >> csv_path >> final_job  # type: ignore


dag = kartverket_punkt_dag()
