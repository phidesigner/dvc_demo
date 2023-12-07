"""
This module provides functionality to load data from various file formats and log it to Weights & Biases.
"""

import os
import logging
import argparse
import pandas as pd
import wandb

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)-15s %(message)s")
logger = logging.getLogger(__name__)


def load_data(file_path: str, sheet_name: str = None) -> pd.DataFrame:
    """
    Load data from a file and return a pandas DataFrame.

    Args:
        file_path (str): Path to the data file.
        sheet_name (str, optional): Name of the sheet if the file is an Excel file. Defaults to None.

    Raises:
        FileNotFoundError: If the file is not found.
        ValueError: If the file format is not supported.

    Returns:
        pd.DataFrame: The loaded data as a pandas DataFrame.
    """
    if not os.path.exists(file_path):
        logger.error("File not found: %s", file_path)
        raise FileNotFoundError(f"File not found: {file_path!r}")

    _, file_extension = os.path.splitext(file_path)

    try:
        if file_extension == '.xlsx':
            logger.info("Loading Excel file from %s", file_path)
            df = pd.read_excel(file_path, sheet_name=sheet_name)
        elif file_extension == '.csv':
            logger.info("Loading CSV file from %s", file_path)
            df = pd.read_csv(file_path)
        elif file_extension == '.json':
            logger.info("Loading JSON file from %s", file_path)
            df = pd.read_json(file_path, orient='split')
        else:
            logger.error("Unsupported file format: %s", file_extension)
            raise ValueError(f"Unsupported file format: {file_extension!r}")

        logger.info("Successfully loaded data from %s", file_path)
        return df

    except pd.errors.EmptyDataError:
        logger.error("No data: %s is empty", file_path)
        raise
    except pd.errors.ParserError:
        logger.error("Parse error: could not parse %s", file_path)
        raise
    except Exception as e:
        logger.error("An error occurred: %s", str(e))
        raise


def main(args):
    """
    Load data from a file and log it to Weights & Biases.

    Args:
        args (argparse.Namespace): Command line arguments.
    """
    wandb.init(job_type="data_loader")

    df = load_data(args.file_path, args.sheet_name)

    artifact = wandb.Artifact(
        args.artifact_name,
        type=args.artifact_type,
        description=args.artifact_description
    )
    with artifact.new_file("raw_data.csv", mode="w") as f:
        df.to_csv(f, index=False)

    wandb.log_artifact(artifact)
    logger.info("Data loaded and artifact logged to Weights & Biases")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load data and log to wandb")
    parser.add_argument(
        "--file_path",
        type=str,
        required=True,
        help="Path to the data file"
    )
    parser.add_argument(
        "--sheet_name",
        type=str,
        help="Sheet name if Excel file"
    )
    parser.add_argument(
        "--artifact_name",
        type=str,
        required=True,
        help="Name for the W&B artifact"
    )
    parser.add_argument(
        "--artifact_type",
        type=str,
        help="Type of the artifact"
    )
    parser.add_argument(
        "--artifact_description",
        type=str,
        help="Description for the artifact"
    )

    args = parser.parse_args()
    main(args)
