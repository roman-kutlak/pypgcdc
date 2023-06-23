--
-- PostgreSQL database dump
--

-- Dumped from database version 12.9
-- Dumped by pg_dump version 14.8 (Homebrew)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: customer; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.customer (
    id bigint NOT NULL,
    fname text,
    lname text,
    dt_updated timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    account_id integer DEFAULT 1 NOT NULL
);


ALTER TABLE public.customer OWNER TO postgres;

--
-- Name: customer_property; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.customer_property (
    id integer NOT NULL,
    account_id integer DEFAULT 1 NOT NULL,
    customer_id integer NOT NULL,
    dt_updated timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    key text,
    value jsonb
);


ALTER TABLE public.customer_property OWNER TO postgres;

--
-- Data for Name: customer; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.customer (id, fname, lname, dt_updated, account_id) FROM stdin;
1	Arthur  Dent	2023-03-12 11:26:29.718691+00	1
2	Ford    Prefect	2023-03-12 11:26:40.815079+00	1
3	Zaphod  Beeblebrox	2023-03-12 16:04:38.08398+00	1
4	Trillian    	2023-03-12 16:58:18.772653+00	1
\.


--
-- Data for Name: customer_property; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.customer_property (id, account_id, customer_id, dt_updated, key, value) FROM stdin;
1	1	1	2023-03-12 17:34:14.673292+00	answer	42
2	1	1	2023-03-12 17:44:25.699917+00	towel	true
3	1	1	2023-03-12 18:11:43.028585+00	heads	"two"
\.


--
-- Name: customer customer_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.customer
    ADD CONSTRAINT customer_pkey PRIMARY KEY (account_id, id);


--
-- Name: customer_property customer_property_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.customer_property
    ADD CONSTRAINT customer_property_pkey PRIMARY KEY (id);


--
-- Name: test_pub; Type: PUBLICATION; Schema: -; Owner: postgres
--

CREATE PUBLICATION test_pub FOR ALL TABLES WITH (publish = 'insert, update, delete, truncate');

ALTER PUBLICATION test_pub OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

