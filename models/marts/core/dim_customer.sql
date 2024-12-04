WITH source AS (
    SELECT *
    FROM {{ ref('stg_google_sheets__customers') }} 
),

data AS (
    SELECT
        customer_id,            -- Clave primaria de la dimensión
        name,
        geo_id,                  -- Nombre del cliente
        country,                -- País del cliente
        gender,                 -- Género del cliente
        phone_number,           -- Número de teléfono
        registration_date,      -- Fecha de registro
        total_spent,            -- Gasto total
        date_of_birth,          -- Fecha de nacimiento
        email,                  -- Correo electrónico
        phone_validation_status, -- Estado de validación del teléfono
        age_years               -- Edad del cliente
    FROM source
    -- Aquí puedes agregar más transformaciones si las necesitas
)

SELECT
    customer_id,            -- Clave primaria de la dimensión
    name,                   -- Nombre del cliente
    country,                -- País del cliente
    gender,                 -- Género del cliente
    phone_number,           -- Número de teléfono
    registration_date,      -- Fecha de registro
    total_spent,            -- Gasto total
    date_of_birth,          -- Fecha de nacimiento
    email,                  -- Correo electrónico
    phone_validation_status, -- Estado de validación del teléfono
    age_years               -- Edad del cliente
FROM data
