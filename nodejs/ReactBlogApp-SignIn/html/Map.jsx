import React from "react";
import L from "leaflet";

const style = {
    width: "100%",
    height: "300px"
};

class Map extends React.Component {
    componentDidMount() {
        // create map
        this.map = L.map("map", {
            center: [49.8419, 24.0315],
            zoom: 16,
            layers: [
                L.tileLayer("http://{s}.tile.osm.org/{z}/{x}/{y}.png", {
                    attribution:
                        '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
                })
            ]
        });

        // add marker
        this.marker = L.marker(this.props.markerPosition).addTo(this.map);
    }
    componentDidUpdate({ markerPosition }) {
        // check if position has changed
        if (this.props.markerPosition !== markerPosition) {
            this.marker.setLatLng(this.props.markerPosition);
        }
    }
    render() {
        return <div id="map" style={style} />;
    }
}

export default Map;
