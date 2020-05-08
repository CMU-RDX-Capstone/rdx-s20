CREATE INDEX idx_map_time ON event_incident_mapping(core_time_num);
CREATE INDEX idx_map_metric ON event_incident_mapping(metric_name);
CREATE INDEX idx_map_evtcd ON event_incident_mapping(event_code);
CREATE INDEX idx_map_recon ON event_incident_mapping(recon_id);
CREATE INDEX idx_map_time ON event_incident_mapping(core_timestamp);
CREATE INDEX idx_map_has_mapped ON event_incident_mapping(has_mapped_incident);

CREATE INDEX idx_inc_time ON incidents(submit_date_num);
CREATE INDEX idx_inc_recon ON incidents(recon_id_inc);
CREATE INDEX idx_inc_evtcd ON incidents(event_code_inc);
CREATE INDEX idx_inc_time ON incidents(submit_date);