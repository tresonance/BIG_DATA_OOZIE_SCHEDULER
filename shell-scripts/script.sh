#!/bin/bash 

# -----------------------------------------------------------------
#
#
#
# -----------------------------------------------------------------



# Function to print a table
print_table() {
  local header=$1
  local description=$2
  local commands=$3

  echo -e "\n# $header"
  echo -e "$description\n"
  echo -e "| Command                                                   | Description                                                                                    |"
  echo -e "|-----------------------------------------------------------|--------------------------------------------------------------------------------|"
  echo -e "$commands" | column -t -s "|"
}

# Job Management Commands
job_commands="
| oozie job -oozie http://localhost:11000/oozie -config <file> -run          | Submits a job with the specified configuration file and starts it.                              |
| oozie job -oozie http://localhost:11000/oozie -info <job_id>              | Retrieves detailed information about the specified Oozie job.                                   |
| oozie job -oozie http://localhost:11000/oozie -log <job_id>               | Retrieves logs for the specified Oozie job.                                                     |
| oozie job -oozie http://localhost:11000/oozie -kill <job_id>              | Kills the specified Oozie job.                                                                  |
| oozie job -oozie http://localhost:11000/oozie -suspend <job_id>           | Suspends a running Oozie job.                                                                  |
| oozie job -oozie http://localhost:11000/oozie -resume <job_id>            | Resumes a suspended Oozie job.                                                                 |
| oozie job -oozie http://localhost:11000/oozie -rerun <job_id>             | Reruns a failed Oozie workflow job. Options like -refresh or -status can be used to control rerun. |
| oozie job -oozie http://localhost:11000/oozie -change <job_id> -value <property=value> | Changes properties (e.g., priority) of a running Oozie job.                                    |
"

print_table "Job Management Commands" "Commands for managing workflow jobs in Oozie." "$job_commands"

# Coordinator Management Commands
coordinator_commands="
| oozie job -oozie http://localhost:11000/oozie -start <coordinator_id> | Starts a coordinator job.                                                                    |
| oozie job -oozie http://localhost:11000/oozie -dryrun <coordinator_id> | Tests the coordinator configuration without executing it.                                    |
| oozie job -oozie http://localhost:11000/oozie -submit <config_file>   | Submits a new coordinator job using the specified configuration file.                        |
"

print_table "Coordinator Management Commands" "Commands for managing coordinator jobs in Oozie." "$coordinator_commands"

# Bundle Management Commands
bundle_commands="
| oozie job -oozie http://localhost:11000/oozie -bundle <config_file>   | Submits a new bundle job using the specified configuration file.                              |
| oozie job -oozie http://localhost:11000/oozie -bundle-info <bundle_id> | Retrieves information about the specified bundle job.                                        |
| oozie job -oozie http://localhost:11000/oozie -bundle-log <bundle_id> | Fetches logs related to a bundle job.                                                        |
"

print_table "Bundle Management Commands" "Commands for managing bundle jobs in Oozie." "$bundle_commands"

# Admin Commands
admin_commands="
| oozie admin -oozie http://localhost:11000/oozie -status      | Checks the status of the Oozie server. Returns System mode: NORMAL if the server is running. |
| oozie admin -oozie http://localhost:11000/oozie -list        | Lists all available services and configurations on the Oozie server.                          |
| oozie admin -oozie http://localhost:11000/oozie -reload      | Reloads Oozie configurations without restarting the server.                                   |
| oozie admin -oozie http://localhost:11000/oozie -os-env      | Displays Oozie's operating system environment variables.                                      |
| oozie admin -oozie http://localhost:11000/oozie -java-env    | Displays Oozie's Java system properties.                                                     |
"

print_table "Admin Commands" "Commands for administering the Oozie server." "$admin_commands"

# Other Useful Commands
other_commands="
| oozie validate <workflow_file>        | Validates an Oozie workflow XML file.                                                         |
| oozie help                            | Displays help for all Oozie commands.                                                         |
| oozie version                         | Displays the version of the Oozie client.                                                     |
"

print_table "Other Useful Commands" "Additional commands for Oozie usage." "$other_commands"

# Options Description
options_description="
| Option                     | Description                                                                                       |
|----------------------------|---------------------------------------------------------------------------------------------------|
| -oozie http://localhost:11000/oozie               | Specifies the Oozie server URL. Default is http://localhost:11000/oozie.                         |
| -config <file>             | Specifies a configuration file for the job or command.                                            |
| -D <property=value>        | Sets properties directly via command-line arguments.                                              |
| -run                       | Submits and starts the job in one command.                                                        |
| -status                    | Retrieves the status of a job.                                                                    |
| -info                      | Fetches detailed information about a job or a component within a job.                             |
| -log                       | Retrieves the logs for a job or its components.                                                   |
"

print_table "Commonly Used Options" "Description of frequently used options for Oozie commands." "$options_description"
