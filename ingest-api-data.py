# This is a basic workflow to help you get started with Actions
name: india_air_quality_json

# Controls when the workflow will run
on:
  # Triggers the workflow on push or pull request events but only for the main branch
   workflow_dispatch:
#on:
 # schedule:
  #   - cron: '45 * * * *'  # Runs every 45th min

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
      - run: |
          home_dir=$(pwd)
          echo $home_dir
          echo -----------------------------------------------------------------------
          pip install --upgrade pip    
          echo -----------------------------------------------------------------------
          pip install "snowflake-snowpark-python[pandas]"
          echo -----------------------------------------------------------------------
          ls -la
          pwd
          echo ----------------------------------------------------------------------- 
          python $home_dir/ingest-api-data.py
