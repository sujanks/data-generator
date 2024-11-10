create table if not exists application_status (
    id uuid,
    application_id varchar(20),
    reason  varchar(200),
    status  varchar(20)
)