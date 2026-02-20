//! Deploy workflow definition (deploy.yml).
//!
//! Note: This workflow is generated manually because the gh-workflow crate
//! doesn't support the `options` field for workflow_dispatch choice inputs.

use serde::{Deserialize, Serialize};

use crate::types::{CloudProvider, Environment, TerraformAction};

/// Creates the deploy workflow as a serializable structure.
pub fn deploy_workflow() -> DeployWorkflow {
    let mut inputs = indexmap::IndexMap::new();

    inputs.insert(
        "environment".to_string(),
        WorkflowDispatchInput {
            description: "Environment to deploy to".to_string(),
            required: true,
            input_type: "choice".to_string(),
            options: Environment::all_str()
                .iter()
                .map(|s| s.to_string())
                .collect(),
        },
    );

    inputs.insert(
        "cloud".to_string(),
        WorkflowDispatchInput {
            description: "Cloud provider".to_string(),
            required: true,
            input_type: "choice".to_string(),
            options: CloudProvider::all_str()
                .iter()
                .map(|s| s.to_string())
                .collect(),
        },
    );

    inputs.insert(
        "action".to_string(),
        WorkflowDispatchInput {
            description: "Action to perform".to_string(),
            required: true,
            input_type: "choice".to_string(),
            options: TerraformAction::all_str()
                .iter()
                .map(|s| s.to_string())
                .collect(),
        },
    );

    let mut env = indexmap::IndexMap::new();
    env.insert("TERRAFORM_VERSION".to_string(), "1.6.0".to_string());

    let mut jobs = indexmap::IndexMap::new();
    jobs.insert("terraform".to_string(), terraform_job());
    jobs.insert("deploy-frogdb".to_string(), deploy_frogdb_job());

    DeployWorkflow {
        name: "Deploy".to_string(),
        on: OnEvent {
            workflow_dispatch: WorkflowDispatch { inputs },
        },
        env,
        jobs,
    }
}

/// Custom workflow structure for deploy.yml
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployWorkflow {
    pub name: String,
    pub on: OnEvent,
    pub env: indexmap::IndexMap<String, String>,
    pub jobs: indexmap::IndexMap<String, Job>,
}

impl DeployWorkflow {
    pub fn to_string(&self) -> Result<String, serde_yaml::Error> {
        serde_yaml::to_string(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnEvent {
    pub workflow_dispatch: WorkflowDispatch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowDispatch {
    pub inputs: indexmap::IndexMap<String, WorkflowDispatchInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowDispatchInput {
    pub description: String,
    pub required: bool,
    #[serde(rename = "type")]
    pub input_type: String,
    pub options: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub name: String,
    #[serde(rename = "runs-on")]
    pub runs_on: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub environment: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub needs: Option<String>,
    #[serde(rename = "if", skip_serializing_if = "Option::is_none")]
    pub cond: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permissions: Option<Permissions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub defaults: Option<Defaults>,
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Permissions {
    #[serde(rename = "id-token", skip_serializing_if = "Option::is_none")]
    pub id_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contents: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Defaults {
    pub run: RunDefaults,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunDefaults {
    #[serde(rename = "working-directory")]
    pub working_directory: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Step {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uses: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run: Option<String>,
    #[serde(rename = "if", skip_serializing_if = "Option::is_none")]
    pub cond: Option<String>,
    #[serde(rename = "with", skip_serializing_if = "Option::is_none")]
    pub with_inputs: Option<indexmap::IndexMap<String, String>>,
}

fn terraform_job() -> Job {
    Job {
        name: "Terraform ${{ inputs.action }} (${{ inputs.cloud }}/${{ inputs.environment }})"
            .to_string(),
        runs_on: "ubuntu-latest".to_string(),
        environment: Some("${{ inputs.environment }}".to_string()),
        needs: None,
        cond: None,
        permissions: Some(Permissions {
            id_token: Some("write".to_string()),
            contents: Some("read".to_string()),
        }),
        defaults: Some(Defaults {
            run: RunDefaults {
                working_directory:
                    "deploy/terraform/${{ inputs.cloud }}/environments/${{ inputs.environment }}"
                        .to_string(),
            },
        }),
        steps: vec![
            Step {
                name: "Checkout".to_string(),
                uses: Some("actions/checkout@v4".to_string()),
                run: None,
                cond: None,
                with_inputs: None,
            },
            Step {
                name: "Setup Terraform".to_string(),
                uses: Some("hashicorp/setup-terraform@v3".to_string()),
                run: None,
                cond: None,
                with_inputs: Some({
                    let mut m = indexmap::IndexMap::new();
                    m.insert(
                        "terraform_version".to_string(),
                        "${{ env.TERRAFORM_VERSION }}".to_string(),
                    );
                    m
                }),
            },
            // AWS Authentication
            Step {
                name: "Configure AWS credentials".to_string(),
                uses: Some("aws-actions/configure-aws-credentials@v4".to_string()),
                run: None,
                cond: Some("inputs.cloud == 'aws'".to_string()),
                with_inputs: Some({
                    let mut m = indexmap::IndexMap::new();
                    m.insert(
                        "role-to-assume".to_string(),
                        "${{ secrets.AWS_ROLE_ARN }}".to_string(),
                    );
                    m.insert(
                        "aws-region".to_string(),
                        "${{ vars.AWS_REGION }}".to_string(),
                    );
                    m
                }),
            },
            // GCP Authentication
            Step {
                name: "Authenticate to Google Cloud".to_string(),
                uses: Some("google-github-actions/auth@v2".to_string()),
                run: None,
                cond: Some("inputs.cloud == 'gcp'".to_string()),
                with_inputs: Some({
                    let mut m = indexmap::IndexMap::new();
                    m.insert(
                        "workload_identity_provider".to_string(),
                        "${{ secrets.GCP_WORKLOAD_IDENTITY_PROVIDER }}".to_string(),
                    );
                    m.insert(
                        "service_account".to_string(),
                        "${{ secrets.GCP_SERVICE_ACCOUNT }}".to_string(),
                    );
                    m
                }),
            },
            // Azure Authentication
            Step {
                name: "Azure Login".to_string(),
                uses: Some("azure/login@v1".to_string()),
                run: None,
                cond: Some("inputs.cloud == 'azure'".to_string()),
                with_inputs: Some({
                    let mut m = indexmap::IndexMap::new();
                    m.insert(
                        "client-id".to_string(),
                        "${{ secrets.AZURE_CLIENT_ID }}".to_string(),
                    );
                    m.insert(
                        "tenant-id".to_string(),
                        "${{ secrets.AZURE_TENANT_ID }}".to_string(),
                    );
                    m.insert(
                        "subscription-id".to_string(),
                        "${{ secrets.AZURE_SUBSCRIPTION_ID }}".to_string(),
                    );
                    m
                }),
            },
            Step {
                name: "Terraform Init".to_string(),
                uses: None,
                run: Some("terraform init".to_string()),
                cond: None,
                with_inputs: None,
            },
            Step {
                name: "Terraform Format Check".to_string(),
                uses: None,
                run: Some("terraform fmt -check".to_string()),
                cond: None,
                with_inputs: None,
            },
            Step {
                name: "Terraform Plan".to_string(),
                uses: None,
                run: Some("terraform plan -out=tfplan".to_string()),
                cond: Some("inputs.action == 'plan' || inputs.action == 'apply'".to_string()),
                with_inputs: None,
            },
            Step {
                name: "Terraform Apply".to_string(),
                uses: None,
                run: Some("terraform apply -auto-approve tfplan".to_string()),
                cond: Some("inputs.action == 'apply'".to_string()),
                with_inputs: None,
            },
            Step {
                name: "Terraform Destroy".to_string(),
                uses: None,
                run: Some("terraform destroy -auto-approve".to_string()),
                cond: Some("inputs.action == 'destroy'".to_string()),
                with_inputs: None,
            },
        ],
    }
}

fn deploy_frogdb_job() -> Job {
    Job {
        name: "Deploy FrogDB".to_string(),
        runs_on: "ubuntu-latest".to_string(),
        environment: Some("${{ inputs.environment }}".to_string()),
        needs: Some("terraform".to_string()),
        cond: Some("inputs.action == 'apply'".to_string()),
        permissions: None,
        defaults: None,
        steps: vec![
            Step {
                name: "Checkout".to_string(),
                uses: Some("actions/checkout@v4".to_string()),
                run: None,
                cond: None,
                with_inputs: None,
            },
            Step {
                name: "Install Helm".to_string(),
                uses: Some("azure/setup-helm@v3".to_string()),
                run: None,
                cond: None,
                with_inputs: Some({
                    let mut m = indexmap::IndexMap::new();
                    m.insert("version".to_string(), "v3.13.0".to_string());
                    m
                }),
            },
            Step {
                name: "Install kubectl".to_string(),
                uses: Some("azure/setup-kubectl@v3".to_string()),
                run: None,
                cond: None,
                with_inputs: None,
            },
            // Get kubeconfig based on cloud provider
            Step {
                name: "Get EKS kubeconfig".to_string(),
                uses: None,
                run: Some(
                    "aws eks update-kubeconfig --name frogdb-${{ inputs.environment }} --region ${{ vars.AWS_REGION }}"
                        .to_string(),
                ),
                cond: Some("inputs.cloud == 'aws'".to_string()),
                with_inputs: None,
            },
            Step {
                name: "Get GKE kubeconfig".to_string(),
                uses: None,
                run: Some(
                    "gcloud container clusters get-credentials frogdb-${{ inputs.environment }} --region ${{ vars.GCP_REGION }}"
                        .to_string(),
                ),
                cond: Some("inputs.cloud == 'gcp'".to_string()),
                with_inputs: None,
            },
            Step {
                name: "Get AKS kubeconfig".to_string(),
                uses: None,
                run: Some(
                    "az aks get-credentials --resource-group frogdb-${{ inputs.environment }} --name frogdb-${{ inputs.environment }}"
                        .to_string(),
                ),
                cond: Some("inputs.cloud == 'azure'".to_string()),
                with_inputs: None,
            },
            Step {
                name: "Deploy FrogDB with Helm".to_string(),
                uses: None,
                run: Some(
                    r#"helm upgrade --install frogdb deploy/helm/frogdb \
  -f deploy/helm/frogdb/values-cluster.yaml \
  --namespace frogdb \
  --create-namespace \
  --set image.tag=${{ github.sha }} \
  --wait --timeout 10m"#
                        .to_string(),
                ),
                cond: None,
                with_inputs: None,
            },
            Step {
                name: "Verify deployment".to_string(),
                uses: None,
                run: Some(
                    "kubectl -n frogdb rollout status statefulset/frogdb --timeout=5m\nkubectl -n frogdb exec frogdb-0 -- redis-cli PING"
                        .to_string(),
                ),
                cond: None,
                with_inputs: None,
            },
        ],
    }
}
