#!/usr/bin/env python3
"""
create_github_role.py
=====================
Creates the AWS IAM role used as AWS_ROLE_ARN_DEV (and optionally staging/prod/dr
variants) for the NextGenDatabridge GitHub Actions CI/CD pipeline.

What it provisions
------------------
1. GitHub Actions OIDC provider  (idempotent — skipped if already present)
2. IAM role assumable by GitHub Actions via OIDC
3. Seven least-privilege managed policies covering:
     ECR          — image push/pull, repository lifecycle
     S3/DynamoDB  — Terraform state bucket + project data buckets + state locking
     EKS          — cluster, node groups, addons, access entries, kubeconfig
     EC2/Network  — VPC, subnets, IGW, NAT, route tables, security groups, ELB
     RDS/Cache/MWAA — RDS (PostgreSQL + SQL Server), ElastiCache Redis, MWAA Airflow
     IAM          — Terraform-managed roles/policies/OIDC providers (project-scoped)
     Secrets/SSM  — Secrets Manager, SSM Parameter Store, CloudWatch Logs,
                    Route 53 (DR failover), KMS, STS

Usage
-----
    # Minimal (account ID auto-detected via STS)
    python scripts/create_github_role.py --repo your-org/nextgen-databridge

    # Full options
    python scripts/create_github_role.py \\
        --repo      your-org/nextgen-databridge \\
        --role-name nextgen-databridge-github-actions-dev \\
        --region    us-east-1 \\
        --env       dev

    # Preview without creating anything
    python scripts/create_github_role.py --repo your-org/nextgen-databridge --dry-run

Usage

pip install boto3

# Dev role
python scripts/create_github_role.py \
  --repo  your-org/nextgen-databridge \
  --env   dev

# Preview without creating
python scripts/create_github_role.py \
  --repo  your-org/nextgen-databridge \
  --env   dev \
  --dry-run

# Staging / production / DR — same script, different --env and --role-name
python scripts/create_github_role.py \
  --repo      your-org/nextgen-databridge \
  --env       staging \
  --role-name nextgen-databridge-github-actions-staging

python scripts/create_github_role.py \
  --repo      your-org/nextgen-databridge \
  --env       production \
  --role-name nextgen-databridge-github-actions-prod \
  --region    us-east-1

python scripts/create_github_role.py \
  --repo      your-org/nextgen-databridge \
  --env       dr \
  --role-name nextgen-databridge-github-actions-dr \
  --region    us-west-2

Requirements
------------
    pip install boto3
    AWS credentials with IAM admin rights (AdministratorAccess or equivalent)

Output
------
    Prints the role ARN.  Copy it and add it as a GitHub secret:
        Settings → Secrets and variables → Actions → New repository secret
        Name:  AWS_ROLE_ARN_DEV
        Value: <printed ARN>
"""

import argparse
import json
import sys
import textwrap
import time

import boto3
from botocore.exceptions import ClientError

# ── Constants ──────────────────────────────────────────────────────────────────

GITHUB_OIDC_HOST = "token.actions.githubusercontent.com"

# GitHub's OIDC intermediate-CA thumbprints (both old and current — include both
# so the provider works regardless of which cert GitHub is presenting)
GITHUB_THUMBPRINTS = [
    "6938fd4d98bab03faadb97b34396831e3780aea1",  # GitHub OIDC (pre-2023)
    "1c58a3a8518e8759bf075b76b750d4f2df264fcd",  # GitHub OIDC (2023+)
]

PROJECT = "nextgen-databridge"
MAX_POLICY_VERSIONS = 5  # AWS hard limit


# ── Policy documents ───────────────────────────────────────────────────────────

def build_policies(account_id: str, region: str) -> dict[str, dict]:
    """
    Returns {policy_name: policy_document} for all seven permission boundaries.
    Each document stays well under the 6,144-character AWS limit.
    """

    def ecr_repo_arns():
        return [f"arn:aws:ecr:{region}:{account_id}:repository/{PROJECT}/*"]

    return {

        # ── 1. ECR ──────────────────────────────────────────────────────────────
        f"{PROJECT}-ecr-policy": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "ECRAuth",
                    "Effect": "Allow",
                    "Action": ["ecr:GetAuthorizationToken"],
                    "Resource": "*",
                },
                {
                    "Sid": "ECRRepoManagement",
                    "Effect": "Allow",
                    "Action": [
                        "ecr:CreateRepository",
                        "ecr:DeleteRepository",
                        "ecr:DescribeRepositories",
                        "ecr:ListRepositories",
                        "ecr:TagResource",
                        "ecr:UntagResource",
                        "ecr:GetRepositoryPolicy",
                        "ecr:SetRepositoryPolicy",
                        "ecr:DeleteRepositoryPolicy",
                        "ecr:PutImageScanningConfiguration",
                        "ecr:PutImageTagMutability",
                        "ecr:PutLifecyclePolicy",
                        "ecr:DeleteLifecyclePolicy",
                        "ecr:GetLifecyclePolicy",
                        "ecr:StartLifecyclePolicyPreview",
                    ],
                    "Resource": ecr_repo_arns(),
                },
                {
                    "Sid": "ECRImageOps",
                    "Effect": "Allow",
                    "Action": [
                        "ecr:BatchCheckLayerAvailability",
                        "ecr:GetDownloadUrlForLayer",
                        "ecr:BatchGetImage",
                        "ecr:PutImage",
                        "ecr:InitiateLayerUpload",
                        "ecr:UploadLayerPart",
                        "ecr:CompleteLayerUpload",
                        "ecr:ListImages",
                        "ecr:DescribeImages",
                        "ecr:BatchDeleteImage",
                        "ecr:DescribeImageScanFindings",
                        "ecr:StartImageScan",
                    ],
                    "Resource": ecr_repo_arns(),
                },
            ],
        },

        # ── 2. S3 (state + project buckets) + DynamoDB (state locking) ──────────
        f"{PROJECT}-s3-dynamodb-policy": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "S3BucketCreate",
                    "Effect": "Allow",
                    "Action": [
                        "s3:CreateBucket",
                        "s3:ListAllMyBuckets",
                        "s3:HeadBucket",
                    ],
                    "Resource": "*",
                },
                {
                    "Sid": "S3BucketConfig",
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetBucketVersioning",
                        "s3:PutBucketVersioning",
                        "s3:GetBucketEncryption",
                        "s3:PutBucketEncryption",
                        "s3:GetPublicAccessBlock",
                        "s3:PutPublicAccessBlock",
                        "s3:GetBucketPolicy",
                        "s3:PutBucketPolicy",
                        "s3:DeleteBucketPolicy",
                        "s3:GetBucketTagging",
                        "s3:PutBucketTagging",
                        "s3:GetBucketCORS",
                        "s3:PutBucketCORS",
                        "s3:GetBucketLocation",
                        "s3:GetLifecycleConfiguration",
                        "s3:PutLifecycleConfiguration",
                        "s3:DeleteBucket",
                        "s3:GetBucketNotification",
                        "s3:PutBucketNotification",
                        "s3:GetBucketLogging",
                        "s3:PutBucketLogging",
                        "s3:GetBucketAcl",
                        "s3:PutBucketAcl",
                        "s3:GetBucketObjectLockConfiguration",
                        "s3:PutBucketObjectLockConfiguration",
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{PROJECT}-terraform-state",
                        f"arn:aws:s3:::{PROJECT}-*",
                    ],
                },
                {
                    "Sid": "S3ObjectOps",
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket",
                        "s3:ListBucketVersions",
                        "s3:DeleteObjectVersion",
                        "s3:GetObjectVersion",
                        "s3:GetObjectTagging",
                        "s3:PutObjectTagging",
                        "s3:AbortMultipartUpload",
                        "s3:ListMultipartUploadParts",
                        "s3:ListBucketMultipartUploads",
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{PROJECT}-terraform-state",
                        f"arn:aws:s3:::{PROJECT}-terraform-state/*",
                        f"arn:aws:s3:::{PROJECT}-*",
                        f"arn:aws:s3:::{PROJECT}-*/*",
                    ],
                },
                {
                    "Sid": "DynamoDBStateLock",
                    "Effect": "Allow",
                    "Action": [
                        "dynamodb:CreateTable",
                        "dynamodb:DeleteTable",
                        "dynamodb:DescribeTable",
                        "dynamodb:GetItem",
                        "dynamodb:PutItem",
                        "dynamodb:DeleteItem",
                        "dynamodb:UpdateItem",
                        "dynamodb:DescribeContinuousBackups",
                        "dynamodb:ListTagsOfResource",
                        "dynamodb:TagResource",
                        "dynamodb:UntagResource",
                        "dynamodb:DescribeTimeToLive",
                    ],
                    "Resource": (
                        f"arn:aws:dynamodb:{region}:{account_id}:table"
                        f"/{PROJECT}-terraform-locks"
                    ),
                },
            ],
        },

        # ── 3. EKS ──────────────────────────────────────────────────────────────
        f"{PROJECT}-eks-policy": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "EKSCluster",
                    "Effect": "Allow",
                    "Action": [
                        "eks:CreateCluster",
                        "eks:DeleteCluster",
                        "eks:DescribeCluster",
                        "eks:ListClusters",
                        "eks:UpdateClusterConfig",
                        "eks:UpdateClusterVersion",
                        "eks:TagResource",
                        "eks:UntagResource",
                        "eks:ListTagsForResource",
                    ],
                    "Resource": (
                        f"arn:aws:eks:{region}:{account_id}:cluster/{PROJECT}-*"
                    ),
                },
                {
                    "Sid": "EKSNodeGroups",
                    "Effect": "Allow",
                    "Action": [
                        "eks:CreateNodegroup",
                        "eks:DeleteNodegroup",
                        "eks:DescribeNodegroup",
                        "eks:ListNodegroups",
                        "eks:UpdateNodegroupConfig",
                        "eks:UpdateNodegroupVersion",
                    ],
                    "Resource": [
                        f"arn:aws:eks:{region}:{account_id}:cluster/{PROJECT}-*",
                        f"arn:aws:eks:{region}:{account_id}:nodegroup/{PROJECT}-*/*/*",
                    ],
                },
                {
                    "Sid": "EKSAddons",
                    "Effect": "Allow",
                    "Action": [
                        "eks:CreateAddon",
                        "eks:DeleteAddon",
                        "eks:DescribeAddon",
                        "eks:ListAddons",
                        "eks:UpdateAddon",
                        "eks:DescribeAddonVersions",
                    ],
                    "Resource": [
                        f"arn:aws:eks:{region}:{account_id}:cluster/{PROJECT}-*",
                        f"arn:aws:eks:{region}:{account_id}:addon/{PROJECT}-*/*/*",
                    ],
                },
                {
                    # Access entries and policies have no resource-level restrictions
                    "Sid": "EKSAccessAndPolicies",
                    "Effect": "Allow",
                    "Action": [
                        "eks:CreateAccessEntry",
                        "eks:DeleteAccessEntry",
                        "eks:DescribeAccessEntry",
                        "eks:ListAccessEntries",
                        "eks:UpdateAccessEntry",
                        "eks:AssociateAccessPolicy",
                        "eks:DisassociateAccessPolicy",
                        "eks:ListAssociatedAccessPolicies",
                        "eks:ListAccessPolicies",
                        "eks:DescribeAccessPolicy",
                    ],
                    "Resource": "*",
                },
            ],
        },

        # ── 4. EC2 / VPC / Networking / Load Balancers ───────────────────────────
        f"{PROJECT}-ec2-networking-policy": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "EC2AndNetworking",
                    "Effect": "Allow",
                    "Action": [
                        # Elastic IPs
                        "ec2:AllocateAddress",
                        "ec2:AssociateAddress",
                        "ec2:DisassociateAddress",
                        "ec2:ReleaseAddress",
                        "ec2:DescribeAddresses",
                        # VPC
                        "ec2:CreateVpc",
                        "ec2:DeleteVpc",
                        "ec2:DescribeVpcs",
                        "ec2:ModifyVpcAttribute",
                        "ec2:DescribeVpcAttribute",
                        # Subnets
                        "ec2:CreateSubnet",
                        "ec2:DeleteSubnet",
                        "ec2:DescribeSubnets",
                        "ec2:ModifySubnetAttribute",
                        # Internet / NAT gateways
                        "ec2:CreateInternetGateway",
                        "ec2:DeleteInternetGateway",
                        "ec2:AttachInternetGateway",
                        "ec2:DetachInternetGateway",
                        "ec2:DescribeInternetGateways",
                        "ec2:CreateNatGateway",
                        "ec2:DeleteNatGateway",
                        "ec2:DescribeNatGateways",
                        # Route tables
                        "ec2:CreateRouteTable",
                        "ec2:DeleteRouteTable",
                        "ec2:DescribeRouteTables",
                        "ec2:AssociateRouteTable",
                        "ec2:DisassociateRouteTable",
                        "ec2:ReplaceRouteTableAssociation",
                        "ec2:CreateRoute",
                        "ec2:DeleteRoute",
                        "ec2:ReplaceRoute",
                        # Security groups
                        "ec2:CreateSecurityGroup",
                        "ec2:DeleteSecurityGroup",
                        "ec2:DescribeSecurityGroups",
                        "ec2:DescribeSecurityGroupRules",
                        "ec2:AuthorizeSecurityGroupIngress",
                        "ec2:AuthorizeSecurityGroupEgress",
                        "ec2:RevokeSecurityGroupIngress",
                        "ec2:RevokeSecurityGroupEgress",
                        "ec2:ModifySecurityGroupRules",
                        "ec2:UpdateSecurityGroupRuleDescriptionsIngress",
                        "ec2:UpdateSecurityGroupRuleDescriptionsEgress",
                        # Tags, metadata
                        "ec2:CreateTags",
                        "ec2:DeleteTags",
                        "ec2:DescribeTags",
                        "ec2:DescribeAvailabilityZones",
                        "ec2:DescribeAccountAttributes",
                        "ec2:DescribeInstances",
                        "ec2:DescribeInstanceTypes",
                        "ec2:DescribePrefixLists",
                        "ec2:DescribeNetworkInterfaces",
                        "ec2:CreateNetworkInterface",
                        "ec2:DeleteNetworkInterface",
                        "ec2:AttachNetworkInterface",
                        "ec2:DetachNetworkInterface",
                        "ec2:DescribeNetworkInterfaceAttribute",
                        "ec2:ModifyNetworkInterfaceAttribute",
                        # Launch templates (EKS node groups)
                        "ec2:DescribeLaunchTemplates",
                        "ec2:DescribeLaunchTemplateVersions",
                        "ec2:CreateLaunchTemplate",
                        "ec2:DeleteLaunchTemplate",
                        "ec2:CreateLaunchTemplateVersion",
                        "ec2:ModifyLaunchTemplate",
                        # VPC Flow Logs
                        "ec2:DescribeFlowLogs",
                        "ec2:CreateFlowLogs",
                        "ec2:DeleteFlowLogs",
                        # Key pairs (read-only)
                        "ec2:DescribeKeyPairs",
                    ],
                    "Resource": "*",
                },
                {
                    "Sid": "ELB",
                    "Effect": "Allow",
                    "Action": [
                        "elasticloadbalancing:CreateLoadBalancer",
                        "elasticloadbalancing:DeleteLoadBalancer",
                        "elasticloadbalancing:DescribeLoadBalancers",
                        "elasticloadbalancing:ModifyLoadBalancerAttributes",
                        "elasticloadbalancing:DescribeLoadBalancerAttributes",
                        "elasticloadbalancing:CreateTargetGroup",
                        "elasticloadbalancing:DeleteTargetGroup",
                        "elasticloadbalancing:DescribeTargetGroups",
                        "elasticloadbalancing:ModifyTargetGroup",
                        "elasticloadbalancing:ModifyTargetGroupAttributes",
                        "elasticloadbalancing:DescribeTargetGroupAttributes",
                        "elasticloadbalancing:RegisterTargets",
                        "elasticloadbalancing:DeregisterTargets",
                        "elasticloadbalancing:DescribeTargetHealth",
                        "elasticloadbalancing:CreateListener",
                        "elasticloadbalancing:DeleteListener",
                        "elasticloadbalancing:DescribeListeners",
                        "elasticloadbalancing:ModifyListener",
                        "elasticloadbalancing:CreateRule",
                        "elasticloadbalancing:DeleteRule",
                        "elasticloadbalancing:DescribeRules",
                        "elasticloadbalancing:ModifyRule",
                        "elasticloadbalancing:AddTags",
                        "elasticloadbalancing:RemoveTags",
                        "elasticloadbalancing:DescribeTags",
                        "elasticloadbalancing:SetSubnets",
                        "elasticloadbalancing:SetSecurityGroups",
                        "elasticloadbalancing:SetIpAddressType",
                    ],
                    "Resource": "*",
                },
            ],
        },

        # ── 5. RDS + ElastiCache + MWAA ─────────────────────────────────────────
        f"{PROJECT}-rds-cache-mwaa-policy": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "RDS",
                    "Effect": "Allow",
                    "Action": [
                        "rds:CreateDBInstance",
                        "rds:DeleteDBInstance",
                        "rds:DescribeDBInstances",
                        "rds:ModifyDBInstance",
                        "rds:RebootDBInstance",
                        "rds:StartDBInstance",
                        "rds:StopDBInstance",
                        "rds:CreateDBSubnetGroup",
                        "rds:DeleteDBSubnetGroup",
                        "rds:DescribeDBSubnetGroups",
                        "rds:ModifyDBSubnetGroup",
                        "rds:CreateDBParameterGroup",
                        "rds:DeleteDBParameterGroup",
                        "rds:DescribeDBParameterGroups",
                        "rds:ModifyDBParameterGroup",
                        "rds:DescribeDBParameters",
                        "rds:ResetDBParameterGroup",
                        "rds:CreateOptionGroup",
                        "rds:DeleteOptionGroup",
                        "rds:DescribeOptionGroups",
                        "rds:ModifyOptionGroup",
                        "rds:DescribeDBEngineVersions",
                        "rds:DescribeOrderableDBInstanceOptions",
                        "rds:AddTagsToResource",
                        "rds:RemoveTagsFromResource",
                        "rds:ListTagsForResource",
                        "rds:DescribeCertificates",
                        "rds:CreateDBSnapshot",
                        "rds:DeleteDBSnapshot",
                        "rds:DescribeDBSnapshots",
                        "rds:RestoreDBInstanceFromS3",
                        "rds:RestoreDBInstanceToPointInTime",
                        "rds:DescribeAccountAttributes",
                    ],
                    "Resource": "*",
                },
                {
                    "Sid": "ElastiCache",
                    "Effect": "Allow",
                    "Action": [
                        "elasticache:CreateCacheCluster",
                        "elasticache:DeleteCacheCluster",
                        "elasticache:DescribeCacheClusters",
                        "elasticache:ModifyCacheCluster",
                        "elasticache:RebootCacheCluster",
                        "elasticache:CreateReplicationGroup",
                        "elasticache:DeleteReplicationGroup",
                        "elasticache:DescribeReplicationGroups",
                        "elasticache:ModifyReplicationGroup",
                        "elasticache:CreateCacheSubnetGroup",
                        "elasticache:DeleteCacheSubnetGroup",
                        "elasticache:DescribeCacheSubnetGroups",
                        "elasticache:ModifyCacheSubnetGroup",
                        "elasticache:CreateCacheParameterGroup",
                        "elasticache:DeleteCacheParameterGroup",
                        "elasticache:DescribeCacheParameterGroups",
                        "elasticache:ModifyCacheParameterGroup",
                        "elasticache:DescribeCacheParameters",
                        "elasticache:CreateCacheSecurityGroup",
                        "elasticache:DeleteCacheSecurityGroup",
                        "elasticache:DescribeCacheSecurityGroups",
                        "elasticache:AddTagsToResource",
                        "elasticache:RemoveTagsFromResource",
                        "elasticache:ListTagsForResource",
                        "elasticache:DescribeCacheEngineVersions",
                        "elasticache:DescribeEngineDefaultParameters",
                    ],
                    "Resource": "*",
                },
                {
                    "Sid": "MWAA",
                    "Effect": "Allow",
                    "Action": [
                        "airflow:CreateEnvironment",
                        "airflow:DeleteEnvironment",
                        "airflow:GetEnvironment",
                        "airflow:ListEnvironments",
                        "airflow:UpdateEnvironment",
                        "airflow:TagResource",
                        "airflow:UntagResource",
                        "airflow:ListTagsForResource",
                        "airflow:CreateWebLoginToken",
                        "airflow:CreateCliToken",
                    ],
                    "Resource": (
                        f"arn:aws:airflow:{region}:{account_id}:environment/{PROJECT}-*"
                    ),
                },
            ],
        },

        # ── 6. IAM — Terraform-managed project roles + OIDC providers ───────────
        f"{PROJECT}-iam-policy": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "IAMRoles",
                    "Effect": "Allow",
                    "Action": [
                        "iam:CreateRole",
                        "iam:DeleteRole",
                        "iam:GetRole",
                        "iam:UpdateRole",
                        "iam:TagRole",
                        "iam:UntagRole",
                        "iam:ListRoleTags",
                        "iam:AttachRolePolicy",
                        "iam:DetachRolePolicy",
                        "iam:PutRolePolicy",
                        "iam:DeleteRolePolicy",
                        "iam:GetRolePolicy",
                        "iam:ListRolePolicies",
                        "iam:ListAttachedRolePolicies",
                        "iam:UpdateAssumeRolePolicy",
                        "iam:ListInstanceProfilesForRole",
                    ],
                    "Resource": [
                        f"arn:aws:iam::{account_id}:role/{PROJECT}-*",
                        # AWS creates service-linked roles automatically; allow Terraform to read them
                        f"arn:aws:iam::{account_id}:role/aws-service-role/*",
                    ],
                },
                {
                    "Sid": "IAMPolicies",
                    "Effect": "Allow",
                    "Action": [
                        "iam:CreatePolicy",
                        "iam:DeletePolicy",
                        "iam:GetPolicy",
                        "iam:GetPolicyVersion",
                        "iam:CreatePolicyVersion",
                        "iam:DeletePolicyVersion",
                        "iam:ListPolicyVersions",
                        "iam:ListPolicies",
                        "iam:TagPolicy",
                        "iam:UntagPolicy",
                    ],
                    "Resource": f"arn:aws:iam::{account_id}:policy/{PROJECT}-*",
                },
                {
                    "Sid": "IAMPassRole",
                    "Effect": "Allow",
                    "Action": "iam:PassRole",
                    "Resource": f"arn:aws:iam::{account_id}:role/{PROJECT}-*",
                    "Condition": {
                        "StringEquals": {
                            "iam:PassedToService": [
                                "eks.amazonaws.com",
                                "ec2.amazonaws.com",
                                "airflow.amazonaws.com",
                                "rds.amazonaws.com",
                                "elasticache.amazonaws.com",
                                "lambda.amazonaws.com",
                            ]
                        }
                    },
                },
                {
                    "Sid": "IAMServiceLinkedRoles",
                    "Effect": "Allow",
                    "Action": "iam:CreateServiceLinkedRole",
                    "Resource": "*",
                    "Condition": {
                        "StringEquals": {
                            "iam:AWSServiceName": [
                                "elasticache.amazonaws.com",
                                "eks.amazonaws.com",
                                "eks-nodegroup.amazonaws.com",
                                "eks-fargate.amazonaws.com",
                                "airflow.amazonaws.com",
                                "rds.amazonaws.com",
                                "elasticloadbalancing.amazonaws.com",
                                "autoscaling.amazonaws.com",
                            ]
                        }
                    },
                },
                {
                    "Sid": "OIDCProviders",
                    "Effect": "Allow",
                    "Action": [
                        "iam:CreateOpenIDConnectProvider",
                        "iam:DeleteOpenIDConnectProvider",
                        "iam:GetOpenIDConnectProvider",
                        "iam:UpdateOpenIDConnectProviderThumbprint",
                        "iam:TagOpenIDConnectProvider",
                        "iam:UntagOpenIDConnectProvider",
                        "iam:ListOpenIDConnectProviders",
                    ],
                    "Resource": "*",
                },
                {
                    "Sid": "IAMInstanceProfiles",
                    "Effect": "Allow",
                    "Action": [
                        "iam:CreateInstanceProfile",
                        "iam:DeleteInstanceProfile",
                        "iam:GetInstanceProfile",
                        "iam:AddRoleToInstanceProfile",
                        "iam:RemoveRoleFromInstanceProfile",
                        "iam:ListInstanceProfiles",
                        "iam:TagInstanceProfile",
                        "iam:UntagInstanceProfile",
                    ],
                    "Resource": f"arn:aws:iam::{account_id}:instance-profile/{PROJECT}-*",
                },
                {
                    # Read-only IAM queries used by Terraform providers
                    "Sid": "IAMReadOnly",
                    "Effect": "Allow",
                    "Action": [
                        "iam:ListRoles",
                        "iam:ListPolicies",
                        "iam:ListOpenIDConnectProviders",
                        "iam:ListInstanceProfiles",
                        "iam:GetUser",
                    ],
                    "Resource": "*",
                },
            ],
        },

        # ── 7. Secrets Manager, SSM, CloudWatch Logs, Route 53, KMS, STS ────────
        f"{PROJECT}-secrets-ssm-monitoring-policy": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "SecretsManager",
                    "Effect": "Allow",
                    "Action": [
                        "secretsmanager:CreateSecret",
                        "secretsmanager:DeleteSecret",
                        "secretsmanager:DescribeSecret",
                        "secretsmanager:GetSecretValue",
                        "secretsmanager:PutSecretValue",
                        "secretsmanager:UpdateSecret",
                        "secretsmanager:TagResource",
                        "secretsmanager:UntagResource",
                        "secretsmanager:ListSecrets",
                        "secretsmanager:GetResourcePolicy",
                        "secretsmanager:PutResourcePolicy",
                        "secretsmanager:DeleteResourcePolicy",
                        "secretsmanager:RotateSecret",
                        "secretsmanager:CancelRotateSecret",
                        "secretsmanager:RestoreSecret",
                    ],
                    "Resource": (
                        f"arn:aws:secretsmanager:{region}:{account_id}:secret:{PROJECT}/*"
                    ),
                },
                {
                    "Sid": "SSM",
                    "Effect": "Allow",
                    "Action": [
                        "ssm:GetParameter",
                        "ssm:GetParameters",
                        "ssm:GetParametersByPath",
                        "ssm:PutParameter",
                        "ssm:DeleteParameter",
                        "ssm:DeleteParameters",
                        "ssm:DescribeParameters",
                        "ssm:AddTagsToResource",
                        "ssm:RemoveTagsFromResource",
                        "ssm:ListTagsForResource",
                    ],
                    "Resource": (
                        f"arn:aws:ssm:{region}:{account_id}:parameter/{PROJECT}/*"
                    ),
                },
                {
                    "Sid": "CloudWatchLogs",
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogGroup",
                        "logs:DeleteLogGroup",
                        "logs:DescribeLogGroups",
                        "logs:DescribeLogStreams",
                        "logs:CreateLogStream",
                        "logs:DeleteLogStream",
                        "logs:PutLogEvents",
                        "logs:GetLogEvents",
                        "logs:FilterLogEvents",
                        "logs:PutRetentionPolicy",
                        "logs:DeleteRetentionPolicy",
                        "logs:AssociateKmsKey",
                        "logs:DisassociateKmsKey",
                        "logs:TagLogGroup",
                        "logs:UntagLogGroup",
                        "logs:TagResource",
                        "logs:UntagResource",
                        "logs:ListTagsLogGroup",
                        "logs:ListTagsForResource",
                    ],
                    "Resource": [
                        f"arn:aws:logs:{region}:{account_id}:log-group:/aws/eks/{PROJECT}-*",
                        f"arn:aws:logs:{region}:{account_id}:log-group:/aws/eks/{PROJECT}-*:*",
                        f"arn:aws:logs:{region}:{account_id}:log-group:/aws/mwaa/{PROJECT}-*",
                        f"arn:aws:logs:{region}:{account_id}:log-group:/aws/mwaa/{PROJECT}-*:*",
                        f"arn:aws:logs:{region}:{account_id}:log-group:/aws/rds/instance/{PROJECT}-*",
                        f"arn:aws:logs:{region}:{account_id}:log-group:/aws/rds/instance/{PROJECT}-*:*",
                        f"arn:aws:logs:{region}:{account_id}:log-group:*{PROJECT}*",
                        f"arn:aws:logs:{region}:{account_id}:log-group:*{PROJECT}*:*",
                    ],
                },
                {
                    "Sid": "Route53ForDRFailover",
                    "Effect": "Allow",
                    "Action": [
                        "route53:CreateHostedZone",
                        "route53:GetHostedZone",
                        "route53:ListHostedZones",
                        "route53:DeleteHostedZone",
                        "route53:ChangeResourceRecordSets",
                        "route53:GetChange",
                        "route53:ListResourceRecordSets",
                        "route53:CreateHealthCheck",
                        "route53:UpdateHealthCheck",
                        "route53:DeleteHealthCheck",
                        "route53:GetHealthCheck",
                        "route53:ListHealthChecks",
                        "route53:ChangeTagsForResource",
                        "route53:ListTagsForResource",
                    ],
                    "Resource": "*",
                },
                {
                    "Sid": "KMS",
                    "Effect": "Allow",
                    "Action": [
                        "kms:CreateKey",
                        "kms:DescribeKey",
                        "kms:GetKeyPolicy",
                        "kms:GetKeyRotationStatus",
                        "kms:ListKeys",
                        "kms:ListAliases",
                        "kms:CreateAlias",
                        "kms:DeleteAlias",
                        "kms:UpdateAlias",
                        "kms:ScheduleKeyDeletion",
                        "kms:EnableKeyRotation",
                        "kms:DisableKeyRotation",
                        "kms:TagResource",
                        "kms:UntagResource",
                        "kms:PutKeyPolicy",
                        "kms:ListResourceTags",
                        "kms:GenerateDataKey",
                        "kms:GenerateDataKeyWithoutPlaintext",
                        "kms:Decrypt",
                        "kms:Encrypt",
                        "kms:ReEncryptFrom",
                        "kms:ReEncryptTo",
                    ],
                    "Resource": "*",
                },
                {
                    "Sid": "STSCallerIdentity",
                    "Effect": "Allow",
                    "Action": "sts:GetCallerIdentity",
                    "Resource": "*",
                },
                {
                    "Sid": "AutoScalingForEKSNodeGroups",
                    "Effect": "Allow",
                    "Action": [
                        "autoscaling:CreateAutoScalingGroup",
                        "autoscaling:DeleteAutoScalingGroup",
                        "autoscaling:DescribeAutoScalingGroups",
                        "autoscaling:UpdateAutoScalingGroup",
                        "autoscaling:DescribeScalingActivities",
                        "autoscaling:DescribeLaunchConfigurations",
                        "autoscaling:CreateLaunchConfiguration",
                        "autoscaling:DeleteLaunchConfiguration",
                        "autoscaling:AttachInstances",
                        "autoscaling:DetachInstances",
                        "autoscaling:PutScalingPolicy",
                        "autoscaling:DeletePolicy",
                        "autoscaling:DescribePolicies",
                        "autoscaling:SetDesiredCapacity",
                        "autoscaling:TerminateInstanceInAutoScalingGroup",
                        "autoscaling:AddTags",
                        "autoscaling:RemoveTags",
                        "autoscaling:DescribeTags",
                    ],
                    "Resource": "*",
                },
            ],
        },
    }


# ── Trust policy ───────────────────────────────────────────────────────────────

def build_trust_policy(account_id: str, repo: str, env: str) -> dict:
    """
    OIDC trust policy allowing GitHub Actions to assume this role.
    Restricts to the specified repo; the sub condition covers all workflow
    triggers (push, PR, workflow_dispatch, workflow_run) on all branches/tags.
    """
    oidc_provider_arn = (
        f"arn:aws:iam::{account_id}:oidc-provider/{GITHUB_OIDC_HOST}"
    )
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "GitHubActionsOIDC",
                "Effect": "Allow",
                "Principal": {
                    "Federated": oidc_provider_arn,
                },
                "Action": "sts:AssumeRoleWithWebIdentity",
                "Condition": {
                    "StringEquals": {
                        f"{GITHUB_OIDC_HOST}:aud": "sts.amazonaws.com",
                    },
                    "StringLike": {
                        # Allow any workflow in this repo; tighten per-env if desired
                        f"{GITHUB_OIDC_HOST}:sub": f"repo:{repo}:*",
                    },
                },
            }
        ],
    }


# ── Idempotent AWS helpers ─────────────────────────────────────────────────────

def ensure_oidc_provider(iam, account_id: str, dry_run: bool) -> str:
    """Creates the GitHub Actions OIDC provider if not already present."""
    oidc_arn = (
        f"arn:aws:iam::{account_id}:oidc-provider/{GITHUB_OIDC_HOST}"
    )
    try:
        iam.get_open_id_connect_provider(OpenIDConnectProviderArn=oidc_arn)
        print(f"  [exists]  OIDC provider: {oidc_arn}")
        return oidc_arn
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            raise
    if dry_run:
        print(f"  [dry-run] Would create OIDC provider for {GITHUB_OIDC_HOST}")
        return oidc_arn
    resp = iam.create_open_id_connect_provider(
        Url=f"https://{GITHUB_OIDC_HOST}",
        ClientIDList=["sts.amazonaws.com"],
        ThumbprintList=GITHUB_THUMBPRINTS,
        Tags=[{"Key": "Project", "Value": PROJECT}],
    )
    oidc_arn = resp["OpenIDConnectProviderArn"]
    print(f"  [created] OIDC provider: {oidc_arn}")
    return oidc_arn


def ensure_role(
    iam,
    role_name: str,
    trust_policy: dict,
    dry_run: bool,
) -> str:
    """Creates the role or updates its trust policy if it already exists."""
    trust_json = json.dumps(trust_policy)
    try:
        resp = iam.get_role(RoleName=role_name)
        role_arn = resp["Role"]["Arn"]
        if not dry_run:
            iam.update_assume_role_policy(
                RoleName=role_name,
                PolicyDocument=trust_json,
            )
        print(f"  [exists]  Role: {role_arn}  (trust policy refreshed)")
        return role_arn
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            raise
    if dry_run:
        print(f"  [dry-run] Would create role: {role_name}")
        return f"arn:aws:iam::ACCOUNT:role/{role_name}"
    resp = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=trust_json,
        Description=(
            "Assumed by GitHub Actions for NextGenDatabridge CI/CD deployments. "
            "Created by create_github_role.py."
        ),
        MaxSessionDuration=3600,
        Tags=[
            {"Key": "Project", "Value": PROJECT},
            {"Key": "ManagedBy", "Value": "create_github_role.py"},
        ],
    )
    role_arn = resp["Role"]["Arn"]
    print(f"  [created] Role: {role_arn}")
    return role_arn


def _cleanup_old_policy_versions(iam, policy_arn: str) -> None:
    """Deletes the oldest non-default policy version to stay under the 5-version limit."""
    versions = iam.list_policy_versions(PolicyArn=policy_arn)["Versions"]
    non_default = sorted(
        [v for v in versions if not v["IsDefaultVersion"]],
        key=lambda v: v["CreateDate"],
    )
    while len(non_default) >= MAX_POLICY_VERSIONS - 1:
        oldest = non_default.pop(0)
        iam.delete_policy_version(
            PolicyArn=policy_arn,
            VersionId=oldest["VersionId"],
        )


def ensure_policy(
    iam,
    account_id: str,
    name: str,
    document: dict,
    dry_run: bool,
) -> str:
    """Creates the managed policy or updates it with a new version if it already exists."""
    policy_arn = f"arn:aws:iam::{account_id}:policy/{name}"
    doc_json = json.dumps(document, separators=(",", ":"))

    # Sanity-check document size
    if len(doc_json) > 6144:
        raise ValueError(
            f"Policy document for '{name}' is {len(doc_json)} chars "
            f"(limit 6,144).  Split it into smaller policies."
        )

    try:
        iam.get_policy(PolicyArn=policy_arn)
        if not dry_run:
            _cleanup_old_policy_versions(iam, policy_arn)
            iam.create_policy_version(
                PolicyArn=policy_arn,
                PolicyDocument=doc_json,
                SetAsDefault=True,
            )
        print(f"  [updated] Policy: {policy_arn}  ({len(doc_json)} chars)")
        return policy_arn
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            raise
    if dry_run:
        print(f"  [dry-run] Would create policy: {name}  ({len(doc_json)} chars)")
        return policy_arn
    resp = iam.create_policy(
        PolicyName=name,
        PolicyDocument=doc_json,
        Description=f"NextGenDatabridge CI/CD permissions — {name}",
        Tags=[
            {"Key": "Project", "Value": PROJECT},
            {"Key": "ManagedBy", "Value": "create_github_role.py"},
        ],
    )
    policy_arn = resp["Policy"]["Arn"]
    print(f"  [created] Policy: {policy_arn}  ({len(doc_json)} chars)")
    return policy_arn


def ensure_policy_attached(
    iam,
    role_name: str,
    policy_arn: str,
    dry_run: bool,
) -> None:
    """Attaches the policy to the role (no-op if already attached)."""
    attached = iam.list_attached_role_policies(RoleName=role_name)[
        "AttachedPolicies"
    ]
    if any(p["PolicyArn"] == policy_arn for p in attached):
        print(f"  [exists]  Attachment: {policy_arn.split('/')[-1]}")
        return
    if dry_run:
        print(f"  [dry-run] Would attach: {policy_arn.split('/')[-1]}")
        return
    iam.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
    print(f"  [attached] {policy_arn.split('/')[-1]}")


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=textwrap.dedent(
            """\
            Create the AWS IAM role for NextGenDatabridge GitHub Actions CI/CD.

            Example:
              python scripts/create_github_role.py \\
                --repo  your-org/nextgen-databridge \\
                --env   dev
            """
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--repo",
        required=True,
        metavar="ORG/REPO",
        help="GitHub repository in owner/name format (e.g. acme/nextgen-databridge)",
    )
    parser.add_argument(
        "--role-name",
        default=f"{PROJECT}-github-actions-dev",
        metavar="NAME",
        help=(
            f"IAM role name (default: {PROJECT}-github-actions-dev). "
            "Use a different name per environment, e.g. …-staging, …-prod, …-dr."
        ),
    )
    parser.add_argument(
        "--env",
        default="dev",
        choices=["dev", "staging", "production", "dr"],
        help="Logical environment label embedded in the role description (default: dev)",
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="AWS region where project resources are deployed (default: us-east-1)",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="AWS CLI profile to use (default: current environment credentials)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be created without making any AWS API calls",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    session = boto3.Session(
        profile_name=args.profile,
        region_name=args.region,
    )
    iam = session.client("iam")
    sts = session.client("sts")

    # Auto-detect account ID
    try:
        account_id = sts.get_caller_identity()["Account"]
    except ClientError as exc:
        print(f"ERROR: Could not determine AWS account ID — {exc}", file=sys.stderr)
        sys.exit(1)

    print(
        f"\n{'DRY RUN — ' if args.dry_run else ''}"
        f"Creating GitHub Actions IAM role for {PROJECT}"
    )
    print(f"  Account : {account_id}")
    print(f"  Region  : {args.region}")
    print(f"  Repo    : {args.repo}")
    print(f"  Role    : {args.role_name}")
    print(f"  Env     : {args.env}")

    # ── Step 1: GitHub OIDC provider ──────────────────────────────────────────
    print("\n[1/4] GitHub OIDC provider")
    ensure_oidc_provider(iam, account_id, args.dry_run)

    # ── Step 2: IAM role ──────────────────────────────────────────────────────
    print("\n[2/4] IAM role")
    trust_policy = build_trust_policy(account_id, args.repo, args.env)
    role_arn = ensure_role(iam, args.role_name, trust_policy, args.dry_run)

    # ── Step 3: Managed policies ──────────────────────────────────────────────
    print("\n[3/4] Managed policies")
    policies = build_policies(account_id, args.region)
    policy_arns = []
    for name, document in policies.items():
        arn = ensure_policy(iam, account_id, name, document, args.dry_run)
        policy_arns.append(arn)

    # ── Step 4: Attach policies to role ───────────────────────────────────────
    print("\n[4/4] Attaching policies to role")
    for arn in policy_arns:
        ensure_policy_attached(iam, args.role_name, arn, args.dry_run)

    # ── Done ──────────────────────────────────────────────────────────────────
    print("\n" + "-" * 60)
    if args.dry_run:
        print("DRY RUN complete — no resources were created.")
    else:
        print("Done!  Role ARN:")
        print(f"\n  {role_arn}\n")
        secret_name = f"AWS_ROLE_ARN_{args.env.upper()}"
        print("Add this as a GitHub repository secret:")
        print(f"  Settings → Secrets and variables → Actions → New repository secret")
        print(f"  Name  : {secret_name}")
        print(f"  Value : {role_arn}")
        print()
        print("Repeat with --env staging / production / dr to create the other roles.")
    print("-" * 60)


if __name__ == "__main__":
    main()
