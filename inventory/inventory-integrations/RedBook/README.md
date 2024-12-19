# RedBook Data Lambda Function

## Overview

The **RedBook Data Lambda Function** is designed to check a database for specific data associated with a given **RedBook code**. If the data is not found in the database, the function retrieves the necessary information from the RedBook API, processes the data, and stores it back into the database.

## Features

- **Database Check**: The function first checks the database to see if data associated with a given RedBook code exists.
- **RedBook API Retrieval**: If data is not found in the database, the function retrieves data from the RedBook API.
- **Data Processing**: Processes the data retrieved from the API to extract relevant information (e.g., equipment names).
- **Database Storage**: Saves the processed data back into the database.

## Data Types

### Input Data
The function expects an event object containing a **RedBook code**.

**Input Example:**

```json
{
  "redbookCode": "XYZ123"
}
```

- **redbookCode** (String): The code obtained trough the Redbook Service

### Output Data
The function returns a JSON response with the status of the operation, either **success** or **failure**, along with the results or an error message.

#### Success Response Example:

```json
{
  "success": true,
  "results": ["Inventory Option 1", "Inventory Option 2", "Inventory Option 3"]
}
```

- **success** (Boolean): Indicates whether the operation was successful.
- **results** (Array of Strings): A list of processed Inventory options.

#### Failure Response Example:

```json
{
  "success": false,
  "message": "No valid data was retrieved from RedBook's API"
}
```