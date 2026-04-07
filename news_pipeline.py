import os

API_KEY = os.getenv("NEWS_API_KEY")

def main():
    if not API_KEY:
        print("NEWS_API_KEY is not set.")
        return

    print("Pipeline starting...")

if __name__ == "__main__":
    main()