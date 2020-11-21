from src.data_pull import ab_pull, bc_pull, on_pull, sk_pull


def run_ab_pull():
    ab_pull.ab_pull_main.main()

def run_bc_pull():
    bc_pull.bc_pull_main.main()
    
def run_on_pull():
    on_pull.on_pull_main.main()
    
def run_sk_pull():
    sk_pull.sk_pull_main.main()


def daily():
    run_ab_pull()
    run_bc_pull()
    run_on_pull()
    run_sk_pull()


if __name__ == "__main__":
    daily()

