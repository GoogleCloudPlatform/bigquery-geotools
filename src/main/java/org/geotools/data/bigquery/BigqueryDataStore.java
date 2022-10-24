package org.geotools.data.bigquery;

import java.io.IOException;
import java.util.List;

import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.opengis.feature.type.Name;

public class BigqueryDataStore extends ContentDataStore {
	
	

	@Override
	protected List<Name> createTypeNames() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
	
}
