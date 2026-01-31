# CLI

Good — this is the **right place to slow down**.
Command-line arguments are the *missing mental piece* for you right now.

I’ll explain this **from zero**, with no dbt assumptions.

---

## 1️⃣ What is a “command-line argument”?

When you type this:

```bash
dbt run

```

You are running a **program** (`dbt`) and giving it **instructions**.

Those extra instructions after the command are called **command-line arguments**.

Example:

```bash
dbt run --models stg_green_tripdata

```

Here:

- `dbt run` → what program + action
- `-models stg_green_tripdata` → **extra instruction**

---

## 2️⃣ What does `-vars` mean?

- `-vars` is a **special argument that dbt understands**.

It lets you pass **temporary values** into dbt **without changing code**.

Example:

```bash
dbt run --vars '{days_back: 7}'

```

This means:

> “For THIS run only, set days_back = 7”
> 

Important:

- It does **not** save anywhere
- It disappears after the command finishes
- Next run → gone

---

## 3️⃣ Where does `-vars` go inside dbt?

Inside dbt, these values are accessed using:

```
var("days_back")

```

So this line:

```
var("days_back", 30)

```

means:

> “If the command line provided days_back, use it.
Otherwise, use 30.”
> 

---

## 4️⃣ Think of `-vars` like shouting at dbt

Imagine dbt asks:

> “How many days back should I look?”
> 

You can **shout back** from the terminal:

```bash
dbt run --vars '{days_back: 7}'

```

dbt hears that **louder than anything else**.

That’s why CLI vars have the **highest priority**.

---

## 5️⃣ How this differs from environment variables

### Environment variables (ENV)

```bash
export DAYS_BACK=14
dbt run

```

These are:

- Set outside dbt
- Often by:
    - Airflow
    - CI
    - dbt Cloud
- Persist until unset

dbt reads them via:

```
env_var("DAYS_BACK")

```

---

### Command-line vars (CLI)

```bash
dbt run --vars '{days_back: 7}'

```

These are:

- Explicit
- Temporary
- Human-controlled
- Highest priority

dbt reads them via:

```
var("days_back")

```

---

## 6️⃣ Why CLI vars are so powerful (and dangerous)

### Power

- Override behavior instantly
- No code changes
- Perfect for debugging

### Danger

- Easy to forget
- Can mask bugs
- Should **never** be relied on in production

That’s why:

- CI & prod use ENV
- Humans use CLI

---

## 7️⃣ Putting it all together (real execution)

You write SQL:

```sql
where pickup_datetime >= CURRENT_DATE
  - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY

```

Then you run:

```bash
dbt run --vars '{days_back: 7}'

```

dbt does:

1. “Do I have CLI var?” → YES → 7
2. Ignore ENV
3. Ignore default

Final SQL:

```sql
INTERVAL '7' DAY

```

---

## 8️⃣ Another example to lock it in

### No CLI, ENV exists

```bash
export DAYS_BACK=14
dbt run

```

dbt:

1. CLI? ❌
2. ENV? ✅ → 14
3. Done

---

### No CLI, no ENV

```bash
dbt run

```

dbt:

1. CLI? ❌
2. ENV? ❌
3. Default → 30

---

## 9️⃣ One-sentence mental model

> Command-line arguments are the loudest voice in dbt.
> 

If you say something explicitly in the terminal, dbt listens to you **over everything else**.

---

## 10️⃣ What you should remember forever

- `-vars` = temporary override
- `var()` reads CLI values
- CLI > ENV > default
- CLI is for humans, ENV is for systems

---

If you want next, I can:

- show you **exactly where CLI vars appear in `dbt compile`**
- explain why dbt doesn’t use ENV directly in SQL
- or show how Airflow passes ENV into dbt jobs