import os
from .functions import aggregate_surveys_config, aggregate_surveys_no_config
from .survey_config import (survey_submits, survey_submits_no_config,
                            get_all_interventions_dict)
from .changed_answers import agg_changed_answers_summary
from typing import Optional, List


def survey_stats_main(
        output_dir: str,
        study_dir: str,
        beiwe_ids: Optional[List] = None,
        config_path: Optional[str] = None,
        time_start: Optional[str] = None,
        time_end: Optional[str] = None,
        study_tz: Optional[str] = "America/New_York",
        interventions_filepath: str = '') -> bool:
    """
    Compute statistics on surveys.

    Args:
    output_dir(str):
        File path to output summaries and details
    study_dir(str):
        File path to study data
    config_path(str):
        File path to study configuration file
    time_start(str):
        The first date of the survey data
    time_end(str):
        The last date of the survey data
    beiwe_ids(list):
        List of users in study for which we are generating a survey schedule
    study_tz(str):
        Timezone of study. This defaults to 'America/New_York'
    interventions_filepath(str):
        filepath where interventions json file is.

    """
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)
    if beiwe_ids is None:
        beiwe_ids = [u for u in os.listdir(
            study_dir) if not u.startswith('.') and u != 'registry']
    # Read, aggregate and clean data
    if config_path is None:
        print('No config file provided. Skipping some summary outputs.')
        agg_data = aggregate_surveys_no_config(study_dir, study_tz, beiwe_ids)
        if agg_data.shape[0] == 0:
            print("Error: No survey data found")
            return(True)
    else:
        agg_data = aggregate_surveys_config(study_dir, config_path,
                                            study_tz, beiwe_ids)
        if agg_data.shape[0] == 0:
            print("Error: No survey data found")
            return(True)
        # Create changed answers detail and summary
        ca_detail, ca_summary = agg_changed_answers_summary(
            study_dir, config_path, agg_data, study_tz)
        ca_detail.to_csv(
            os.path.join(
                output_dir,
                'answers_data.csv'),
            index=False)
        ca_summary.to_csv(
            os.path.join(
                output_dir,
                'answers_summary.csv'),
            index=False)
        if time_start is not None and time_end is not None:
            # Create survey submits detail and summary
            all_interventions_dict = get_all_interventions_dict(
                interventions_filepath)
            ss_detail, ss_summary = survey_submits(
                study_dir, config_path, time_start, time_end, beiwe_ids,
                agg_data, study_tz, all_interventions_dict)
            if ss_summary.shape[0] > 0:
                ss_detail.to_csv(
                    os.path.join(
                        output_dir,
                        'submits_data.csv'),
                    index=False)
                ss_summary.to_csv(
                    os.path.join(
                        output_dir,
                        'submits_summary.csv'),
                    index=False)
            else:
                print("An Error occurred when getting survey submit summaries")
    # Write out summaries
    agg_data.to_csv(
        os.path.join(
            output_dir,
            'agg_survey_data.csv'),
        index=False)
    # Add alternative survey submits table
    submits_tbl = survey_submits_no_config(study_dir, study_tz)
    submits_tbl.to_csv(
        os.path.join(
            output_dir,
            'submits_alt_summary.csv'),
        index=False)
    return(True)
