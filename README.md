# Simwrapper Dashboard and Summary File Configuration for Topsheet Generation

## Overview
This section describes the process for configuring and executing scripts to generate the datasets required to create Topsheet Simwrapper dashboards.


## Quick Setup Example

To streamline the setup process and reduce the need for manual configuration and file transfers, you should directly download the entire GitHub repository for each model run. Here’s how to proceed if you're setting up a dashboard within the directory `X:\Projects\CHAMP7\Run25w_Market`:

1. **Create a New Folder**:
   - Copy `folders_config.txt` and `topsheet_run.bat` into a folder

2. **Modify Configuration Files**:
   - In the `folders_config.txt` file, set the following:
     - `WORKING_FOLDER` to `X:\Projects\CHAMP7\Run25w_Market`. I strongly recommend using an absolute path for accuracy and consistency.
     - `OUTPUT_FOLDER` to `X:\Projects\CHAMP7\Run25w_Market\dc_test\topsheet\data`. Again, using an absolute path is highly recommended for best results.
     - `SCRIPTS_FOLDER`(Typically, you don't have to change this part.)

3. **Execution**:
   - Execute `topsheet_run.bat`  to view the dashboard.

For more information and to check for potential errors, please view 

[Scripts documentation](https://docs.google.com/document/d/1d1rsOzBTJeTjgL5Qi_pyLSJCFsjHB4bm8BG8Yhs29OA/edit).

[Dashboard design documentation](https://docs.google.com/document/d/17FlH8S7F_nT-qn5nNHcWCI0Av0-2xLipeVUh8ym7Hh0/edit).

[Dashboard review documentation](https://docs.google.com/document/d/1o2vDNIshKRgV7FeATWGQIALQo78pNWbwqu3ksiplu28/edit).

