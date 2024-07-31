# Symbolic Data Partitioning

## Overview

This repository contains the code for Catalyst. It includes the entire codebase. Follow the steps below to build and run the system, and to conduct experiments.

## Building the System

To build the system, follow these steps:

1. **Build the Docker image:**

   ```bash
   docker build -t artifactimg .
2. ** Run the Docker image:**
   ```bash
   docker run -it artifactimg /bin/bash

Alternatively, the system can be compiled by simply running mvn clean install. However, openSMT and approxmc need to be installed appropriately and their binaries need to be included in /resources. 

## Running Experiments

### Experiment 1: Distributed Transactions, Execution Time, Graph Generation


