
- **Theoretical Calculations**: The Jupyter notebook named "Theoretical calculations (Table 1, Figure 4 to 6).ipynb" contains the code for the theoretical calculations referenced in the paper.

- **Experimental Setup**: For the experiments discussed in Section 7, we have open-sourced our code. Please use the following steps to reproduce the results:

  - **AWS CLI Setup**: Before initiating the testbed setup, please ensure the AWS CLI is installed and configured on your system. Follow these steps:
    1. **Install AWS CLI**: Download and install the AWS CLI from the official [AWS website](https://aws.amazon.com/cli/).
    2. **Configure AWS CLI**: Run `aws configure` in your command line. Enter your AWS Access Key, Secret Key, region, and output format as prompted.

  - **Creating IAM User and Role**:
    1. **Create IAM User**: Name the user 'Experiment'. This user will need specific permissions to operate within AWS for our experiments.
    2. **Attach Policies**: Apply the following permissions to ensure proper functionality (see Experiment__ and the two screenshots for details):
       - Amazon EC2
       - Amazon S3
       - AWS Systems Manager (SSM)
       - Amazon CloudWatch
       - IAM permissions for role management
    3. **Create Role**: Create a role named 'Experiment_' and associate it with the 'Experiment' user. This role allows the user to perform actions such as running instances and managing other AWS resources.
    4. **Trusted Entities**: Allow the 'Experiment_' role to be assumed by AWS services, specifically the EC2 service.

  - **Testbed Setup**: The "Testbed_setup.ipynb" notebook facilitates the replication of our experimental environment. It automates the setup process, including the initiation of thousands of EC2 instances and the construction of networks, gateways, and routing tables across 17 AWS regions. Please ensure your AWS service quotas allow for a maximum number of vCPUs assigned to the Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances to be increased to 110 to replicate our setup accurately.

  - **Data Generation for Analysis**: After setting up the testbed, transfer the "vpc_info" from "Testbed_setup.ipynb" to "Start_test_.ipynb". This notebook executes the process of calculating time-bounds concerning different scales of nodes and if Byzantine broadcasting is used. As one sharded BFT consensus system in synchronous communication networks, calculating the time-bounds is the most important thing to ensure security and performance as the procedures must complete within the time-bound. If finished earlier, it needs to wait until the time-bound for safety. This notebook provides simulations for PBFT (all protocols we compared use PBFT as the intra-shard consensus protocol) and PBFT with BRB, which are utilized in generating the data for Figure 7. Note: PBFT with BRB is currently a semi-finished product. For accurate costing, consider the current PBFT with BRB cost plus the cost of two additional PBFT instances.
