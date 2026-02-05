from setuptools import setup, find_packages

setup(
    name="google_cloud_utils",
    version="0.1.3.5",
    packages=find_packages(),
    install_requires=[
        "google-cloud-bigquery",
        "google-cloud-storage",
        "google-cloud-secret-manager",
        "google-cloud-tasks",
        "google-cloud-scheduler",
        "google-cloud-datastore",
        "google-cloud-firestore",
        "google-cloud-pubsub",
        "pandas",
        "python-dotenv",
        "genson",
        "regex",
        "numpy"
    ],
    author="DataGem Consulting",
    description="A wrapper for Google Cloud services handlers",
    python_requires=">=3.8",
)
