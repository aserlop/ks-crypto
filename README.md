# ks-crypto

<!-- TABLE OF CONTENTS -->
## Table of Contents

* [About the Project](#about-the-project)
* [Getting Started](#getting-started)
  * [Prerequisites](#prerequisites)
  * [Launch](#launch)
* [Roadmap](#roadmap)
* [Project Report](#project-report)
* [Process Diagrams](#process-diagrams)



<!-- ABOUT THE PROJECT -->
## About The Project

In this project we offer an end-to-end in-cloud data science project for detecting illicit activity in Bitcoin 
transactional market. For this purpose we use a Temporal Graph Convolutional Network 
([T-GCN](https://arxiv.org/abs/1811.05320)). However this project can be used to generate a labeled dataset of the
 bitcoin transactional market so feel free to use it and run your own experiments. 

<!-- GETTING STARTED -->
## Getting Started

To get this project running follow these simple steps.

### Prerequisites

This is a list of the things you need to prepare before starting:

* Clone this repository using the following command.
```sh
git clone --branch develop https://github.com/aserlop/ks-crypto.git
```
* Register yourself at GCP
* Create a project in GCP
* Create a Bucket for the project at GCP
* Create a dataset in GCP to export the intermediate tables
* Install GDK at your local computer
* Upload the Kaggle datasets using the notebook export_kaggle_data_to_bigquery.ipynb in google colab

### Launch

1. Move to ./bin directory using the cd command.
2. Edit the launch_ks_crypto.sh adding your specific configuration.
3. Comment the steps that you dont want to run in launch_ks_crypto.sh (i.e. 05_close_dataproc.sh). 
4. Use the sh command to launch launch_ks_crypto.sh 
5. You can monitor the application as usual as a normal yarn job
6. At the end you will have a trained model in your bucket.

<!-- ROADMAP -->
## Roadmap

Many of the future features that the code will include are:

* Add some dense layers to include more variables in the modeling.
* Use Attention Layers instead of GRU.
* Validate this model with the network of another cryptocurrency.
* Represent the embeddings usin hyperbolic spaces.


<!-- CONTRIBUTING -->
## Contributing

Given that the project creates a framework to use this data, the community can use it to develop new solutions 
to this problem, It is thought to be used for modeling as a dynamic graph, whereas the intermediary tables generated 
make it possible to develop any kind of predictive algorithm. Any contributions you make are **greatly appreciated**. 
For a new contribution follow these steps:

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

<!-- PROJECT-REPORT  -->
## Project Report

Currently a report of the project is available 
[here](https://docs.google.com/document/d/1mCI7Mgm6Ola-0K_R62Zp9L8sbiZOdReH9bjXm7YR28I/edit?usp=sharing) 
(only in spanish).

<!-- PROCESS-DIAGRAMS  -->
## Process Diagrams

In order to clarify the whole process a set of diagrams are provided, these correspond with the different bash scripts 
that execute the project:

* launch_ks_crypto.sh (whole proccess): [download](https://drive.google.com/file/d/1lyp4KZVIgmlUB7ZvpUEMIyCZw3cWSwl_/view?usp=sharing)
* 00_start_dataproc.sh: [download](https://drive.google.com/file/d/19xcYXiZG0-iM2nBDxnfD6Fn5Ky0l1kGZ/view?usp=sharing)
* 01_extract_data.sh: [download](https://drive.google.com/file/d/1V4t3M0wj9i0q259h5LV0FXxK8TfxrD5S/view?usp=sharing)
* 02_filter_data.sh: [download](https://drive.google.com/file/d/1cjRBvkpmD9BL2nPZ--fkm5rIvF_CcFMp/view?usp=sharing)
* 03_feature_engineering.sh: [download](https://drive.google.com/file/d/1OHsEBxdcuKGLv6F9hVHEvbbf0tA4hfzz/view?usp=sharing)
* 04_modeling.sh: [download](https://drive.google.com/file/d/1yQyCQdTNAUob5jo3cB0SMulwDAhzS1Ni/view?usp=sharing)

