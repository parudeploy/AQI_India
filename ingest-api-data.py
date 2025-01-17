# This is a basic workflow to help you get started with Actions
name: india_air_quality_json

# Controls when the workflow will run
on:
  # Triggers the workflow on push or pull request events but only for the main branch
   workflow_dispatch:
#on:
 # schedule:
 #    - cron: '45 * * * *'  # Runs every 45th min

# A workflow run is made up of one or more jobs that can run sequentially or in parallel
jobs:
  # This workflow contains a single job called "build"
  build:
    # The type of runner that the job will run on
    runs-on: ubuntu-latest

     steps:
      - uses: actions/checkout@v3

      # Set up Python 3.8
      - name: Set up Python 3.8
        uses: actions/setup-python@v2
        with:
          python-version: 3.8  # Ensure using Python 3.8

      # Install pip and dependencies
      - name: Install dependencies
        run: |
          home_dir=$(pwd)
          echo "Home directory: $home_dir"
          echo -----------------------------------------------------------------------
          pip install --upgrade pip  # Upgrade pip
          echo -----------------------------------------------------------------------
          
          # Install a specific version of snowflake-snowpark-python (compatible with Python 3.8)
          pip install "snowflake-snowpark-python==1.4.0"  # Specific version for Python 3.8
          # If you need pandas, install it separately:
          pip install pandas

          echo -----------------------------------------------------------------------
          ls -la
          pwd
          echo -----------------------------------------------------------------------
          
      # Run the Python script
      - name: Run ingest-api-data.py
        run: |
          python $home_dir/ingest-api-data.py
