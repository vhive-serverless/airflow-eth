import argparse
import remote_xcomm as rx


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--url', type=str, default="localhost")
    args = parser.parse_args()

    data = {
        "others": {
            "mark_success": True,
            "test_mode": False,
            "job_id": "1",
            "pool": None,
        },
        "payloads": "phw"
    }

    response = rx.requests(url=args.url, port=80, data=data)
    print(response)
