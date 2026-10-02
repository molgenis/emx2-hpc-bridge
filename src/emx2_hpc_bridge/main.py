from .config import Config

def main():
    print(f"Connecting to {Config.emx2_server} with schema {Config.emx2_schema}")
    print(f"Polling every {Config.poll_interval} seconds")

if __name__ == "__main__":
    main()