PROMPT turn off RAIS Void Event
ALTER trigger EVT_BR_IU disable;
UPDATE event_types
SET    evt_current  = 'N'
WHERE  evt_code     = 'RAIS'
/
ALTER trigger EVT_BR_IU enable;
