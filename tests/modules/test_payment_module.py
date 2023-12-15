from modules.payment import Payment


def test_payment_stg():
    payment = Payment()
    df = payment.load_stg_s()
    assert df is not None


def test_payment_fact():
    payment = Payment()
    df = payment.load_fact_payment()
    assert df is not None

