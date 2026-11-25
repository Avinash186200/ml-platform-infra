output "vpc_id" {
  description = "VPC id."
  value       = aws_vpc.this.id
}

output "vpc_cidr_block" {
  description = "VPC CIDR."
  value       = aws_vpc.this.cidr_block
}

output "private_subnet_ids" {
  description = "Private subnets (nodes, Redis)."
  value       = aws_subnet.private[*].id
}

output "public_subnet_ids" {
  description = "Public subnets (internet-facing load balancers, NAT)."
  value       = aws_subnet.public[*].id
}

output "s3_endpoint_id" {
  description = "Gateway endpoint id for S3."
  value       = aws_vpc_endpoint.s3.id
}
