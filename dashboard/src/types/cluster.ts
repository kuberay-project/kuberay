export type clusterType = "dev" | "prod" | "stage" | "infra";
export type region = "us-east-1" | "us-east-2";

export interface cluster {
  cluster: clusterType;
  region: region;
}
