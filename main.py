# module used to wrap whole pipeline
from etl_process import AmazonScrapeGPU
from analysis_process import gpu_analysis_dashboard

if __name__ == "__main__":
    AmazonScrapeGPU().run_etl_pipeline()
    gpu_analysis_dashboard()
