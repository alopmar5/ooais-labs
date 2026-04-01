import json
import csv

# Step 3: Define File Paths
DATASET_PATH = "data/raw/orbital_observations.csv"
METADATA_PATH = "data/raw/metadata.json"

def ingest_data():
    # Step 4: Load Metadata
    with open(METADATA_PATH, 'r') as f:
        metadata = json.load(f)

    # Step 5: Load Dataset
    rows = []
    dataset_columns = []
    
    with open(DATASET_PATH, 'r') as f:
        # Usamos DictReader para que cada fila sea un diccionario
        reader = csv.DictReader(f)
        dataset_columns = reader.fieldnames # Obtener nombres de columnas
        for row in reader:
            rows.append(row)

    # Step 6: Inspect Loaded Data
    print(f"Dataset: {metadata['dataset_name']}")
    print(f"Records loaded: {len(rows)}")
    print(f"Columns (dataset): {dataset_columns}")
    print(f"Columns (metadata): {metadata['columns']}")

#Task5: Column Consistency Validation

# Comparamos si las columnas del CSV coinciden exactamente con el JSON
    if dataset_columns == metadata['columns']:
        print("Column validation: OK")
    else:
        # Si hay un error, mostramos qué falta o qué sobra
        print("Column validation: MISMATCH")
        print(f"Expected: {metadata['columns']}")
        print(f"Actual: {dataset_columns}")

#Task6: Record Count Validation
    actual_count = len(rows)
    expected_count = metadata["num_records"]

    if actual_count == expected_count:
        print("Record count: OK")
    else:
        print("Record count: MISMATCH")
        print(f"Expected: {expected_count}")
        print(f"Actual: {actual_count}")


#Task7: Detecting Invalid Records
    valid_records = []
    invalid_records = []

    for row in rows:
        # Verificamos si la temperatura es "INVALID"
        if row['temperature'] == 'INVALID':
            invalid_records.append(row)
        else:
            valid_records.append(row)

    # Imprimimos los resultados del filtrado
    print(f"Valid: {len(valid_records)}")
    print(f"Invalid: {len(invalid_records)}")

    # Comparación con los metadatos
    expected_invalid = metadata["invalid_records"]
    if len(invalid_records) == expected_invalid:
        print("Invalid records alignment: OK")
    else:
        print("Invalid records alignment: MISMATCH")
        print(f"Expected: {expected_invalid}")
        print(f"Actual: {len(invalid_records)}")

#Task8: Saving Processed Outputs

    VALID_OUTPUT_PATH = "data/processed/observations_valid.csv"
    INVALID_OUTPUT_PATH = "data/processed/observations_invalid.csv"

    # Guardar registros válidos
    with open(VALID_OUTPUT_PATH, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=dataset_columns)
        writer.writeheader()
        writer.writerows(valid_records)

    # Guardar registros inválidos
    with open(INVALID_OUTPUT_PATH, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=dataset_columns)
        writer.writeheader()
        writer.writerows(invalid_records)

    print(f"Generated: {VALID_OUTPUT_PATH}")
    print(f"Generated: {INVALID_OUTPUT_PATH}")

#Task9: Preparing Data for Preprocessing and Model Input

    MODEL_INPUT_PATH = "data/processed/model_input.csv"
    feature_cols = metadata["feature_columns"]

    with open(MODEL_INPUT_PATH, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=feature_cols)
        writer.writeheader()

        for row in valid_records:
            filtered_row = {col: row[col] for col in feature_cols}
            writer.writerow(filtered_row)
 
    print(f"Generated: {MODEL_INPUT_PATH}")


#Task10: Ingestion Summary and Data Quality Reporting

    REPORT_PATH = "reports/ingestion_summary.txt"
    
    # Recopilación de resultados en variables
    col_status = "OK" if dataset_columns == metadata['columns'] else "MISMATCH"
    count_status = "OK" if len(rows) == metadata['num_records'] else "MISMATCH"
    
    summary_content = [
        f"Dataset: {metadata['dataset_name']}",
        f"Records loaded: {len(rows)}",
        f"Expected records: {metadata['num_records']}",
        f"Column validation: {col_status}",
        f"Record count validation: {count_status}",
        f"Valid records: {len(valid_records)}",
        f"Invalid records: {len(invalid_records)}",
        "Generated files:",
        f"- {VALID_OUTPUT_PATH}",
        f"- {INVALID_OUTPUT_PATH}",
        f"- {MODEL_INPUT_PATH}"
    ]

    with open(REPORT_PATH, 'w') as f:
        f.write("\n".join(summary_content))

    print(f"Generated: {REPORT_PATH}")


#P3: Metadata Consistency 
    feature_columns = metadata["feature_columns"]
    target_column = metadata["target_column"]
    
    # Validar features
    missing_features = [col for col in feature_columns if col not in dataset_columns]
    if not missing_features:
        print("Feature validation: OK")
    else:
        print(f"Missing feature columns: {missing_features}")

    # Validar target
    if target_column in dataset_columns:
        print("Target validation: OK")
    else:
        print(f"Target column missing: {target_column}")



if __name__ == "__main__":
    ingest_data()
