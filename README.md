# Unity Catalog Unused Tables Reporter

This repository contains a Databricks-ready tool to identify unused or rarely used tables within a Unity Catalog.

## Overview

The tool queries the Unity Catalog System Tables (`system.access.table_usage` and `system.information_schema.tables`) to generate a report. It calculates how long it has been since each table was last accessed and identifies tables that have no access usage records.

## Prerequisites

1.  **Unity Catalog**: Your workspace must be enabled for Unity Catalog.
2.  **System Tables**: System tables (specifically `system.access` and `system.information_schema`) must be enabled and accessible by the user running the notebook.

## How to Use

1.  **Clone this Repo**: Clone this repository into your Databricks Workspace (using Git Folders / Repos).
2.  **Open the Notebook**: Navigate to `notebooks/find_unused_tables`.
3.  **Run the Notebook**:
    *   You will be prompted to enter the **Catalog Name** in the widget at the top.
    *   Enter the name of the catalog you wish to scan.
    *   Run all cells.
4.  **Export to PDF**:
    *   Review the generated charts and data tables.
    *   To save the report, go to the Databricks menu: `File` -> `Export` -> `HTML` (then print to PDF) or direct `PDF` export if available in your workspace version.

## Structure

*   `notebooks/`: Contains the entry point notebook to run the analysis.
*   `src/`: Contains the Python source code for data analysis and visualization.
