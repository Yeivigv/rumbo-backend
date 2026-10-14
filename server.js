import { WebSocketServer } from 'ws';
import crypto from 'crypto';

// Usamos el puerto dinámico que nos asigna Render automáticamente
const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

const drivers = new Map();
const connectionDrivers = new Map();
const groups = new Map();
const driverGroups = new Map();

function groupSnapshot() {
  return [...groups.values()].map(group => ({
    id: group.id,
    name: group.name,
    adminId: group.adminId,
    adminName: group.adminName,
    route: group.route,
    distance: group.distance,
    routeMode: group.routeMode,
    members: [...group.members.values()].map(m => ({
      id: m.id,
      name: m.name,
      arrivalStatus: m.arrivalStatus || "En ruta",
      queuePosition: m.queuePosition || null,
      arrivedAt: m.arrivedAt || null
    })).sort((a, b) => {
      if (!a.arrivedAt) return 1;
      if (!b.arrivedAt) return -1;
      return a.arrivedAt - b.arrivedAt;
    })
  }));
}

function broadcast(data) {
  const message = JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client.readyState === 1) { 
      client.send(message);
    }
  });
}

function routeDistance(points) {
  const radius = 6371000;
  let distance = 0;
  for (let i = 1; i < points.length; i++) {
    const a = points[i - 1], b = points[i];
    const lat1 = a.lat * Math.PI / 180, lat2 = b.lat * Math.PI / 180;
    const dLat = lat2 - lat1, dLon = (b.lon - a.lon) * Math.PI / 180;
    const haversine = Math.sin(dLat / 2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
    distance += 2 * radius * Math.asin(Math.sqrt(Math.min(1, haversine)));
  }
  return Math.round(distance);
}

function recalcularOrdenSalida(group) {
  const conductoresDisponibles = [...group.members.values()]
    .filter(member => member.arrivedAt !== null && member.arrivedAt !== undefined)
    .sort((a, b) => a.arrivedAt - b.arrivedAt);

  conductoresDisponibles.forEach((member, index) => {
    const m = group.members.get(member.id);
    m.queuePosition = index + 1;
    group.members.set(member.id, m);
  });
}

wss.on('connection', (ws) => {
  ws.id = crypto.randomUUID();
  
  ws.send(JSON.stringify({ type: "locations", locations: [...drivers.values()].map(({ connectionId, ...l }) => l) }));
  ws.send(JSON.stringify({ type: "groups-state", groups: groupSnapshot() }));

  ws.on('message', (message) => {
    let input;
    try { input = JSON.parse(message); } catch { return; }

    if (input.type === "driver-location") {
      const driverId = String(input.driverId ?? "").slice(0, 100);
      const lat = Number(input.lat);
      const lon = Number(input.lon);
      if (!driverId || !Number.isFinite(lat) || !Number.isFinite(lon) || Math.abs(lat) > 90 || Math.abs(lon) > 180) return;
      
      const location = {
        driverId,
        name: String(input.name ?? "Conductor").slice(0, 60),
        lat: Math.round(lat * 1e6) / 1e6,
        lon: Math.round(lon * 1e6) / 1e6,
        updatedAt: Date.now(),
        connectionId: ws.id
      };
      drivers.set(driverId, location);
      connectionDrivers.set(ws.id, driverId);
      
      const { connectionId, ...publicLocation } = location;
      broadcast({ type: "location-update", location: publicLocation });
      return;
    }

    if (input.type === "group-create" || input.type === "group-join" || input.type === "group-leave" || input.type === "group-route" || input.type === "driver-checkpoint-arrival" || input.type === "group-dispatch-first") {
      const driverId = connectionDrivers.get(ws.id);
      const driver = driverId && drivers.get(driverId);
      if (!driver || driver.connectionId !== ws.id) return;
      const member = { id: driverId, name: driver.name };
      const currentGroupId = driverGroups.get(driverId);

      if (input.type === "group-create") {
        if (currentGroupId) return ws.send(JSON.stringify({ type: "group-error", message: "Ya formas parte de un grupo." }));
        const id = String(input.id ?? "").slice(0, 100);
        if (!id || groups.has(id)) return;
        const group = { id, name: String(input.name ?? "Nuevo grupo").slice(0,60), adminId: driverId, adminName: driver.name, members: new Map([[driverId, member]]), route: [], distance: 0, routeMode: 'manual' };
        groups.set(id, group);
        driverGroups.set(driverId, id);
        broadcast({ type: "groups-state", groups: groupSnapshot() });
        return;
      }

      if (input.type === "group-join") {
        if (currentGroupId) return ws.send(JSON.stringify({ type: "group-error", message: "Sal de tu grupo actual antes de unirte a otro." }));
        const group = groups.get(String(input.groupId ?? ""));
        if (!group) return ws.send(JSON.stringify({ type: "group-error", message: "Ese grupo ya no está disponible." }));
        group.members.set(driverId, member);
        driverGroups.set(driverId, group.id);
        broadcast({ type: "groups-state", groups: groupSnapshot() });
        return;
      }

      if (input.type === "group-leave") {
        const group = groups.get(currentGroupId);
        if (!group) return;
        group.members.delete(driverId);
        driverGroups.delete(driverId);
        if (!group.members.size) groups.delete(group.id);
        else if (group.adminId === driverId) {
          const nextAdmin = group.members.values().next().value;
          group.adminId = nextAdmin.id;
          group.adminName = nextAdmin.name;
        }
        broadcast({ type: "groups-state", groups: groupSnapshot() });
        return;
      }

      const group = groups.get(currentGroupId);
      if (!group) return;

      if (input.type === "driver-checkpoint-arrival") {
        const memberInGroup = group.members.get(driverId);
        if (memberInGroup) {
          memberInGroup.arrivalStatus = input.checkpoint;
          memberInGroup.arrivedAt = Date.now();
          group.members.set(driverId, memberInGroup);
          recalcularOrdenSalida(group);
          broadcast({ type: "groups-state", groups: groupSnapshot() });
        }
        return;
      }

      if (group.adminId !== driverId) return ws.send(JSON.stringify({ type: "group-error", message: "Solo el administrador puede editar el grupo." }));

      if (input.type === "group-dispatch-first") {
        const listaOrdenada = [...group.members.values()].filter(m => m.arrivedAt).sort((a, b) => a.arrivedAt - b.arrivedAt);
        if (listaOrdenada.length > 0) {
          const primerConductor = group.members.get(listaOrdenada.id);
          primerConductor.arrivalStatus = "En ruta";
          primerConductor.arrivedAt = null;
          primerConductor.queuePosition = null;
          group.members.set(primerConductor.id, primerConductor);
          recalcularOrdenSalida(group);
          broadcast({ type: "groups-state", groups: groupSnapshot() });
        }
        return;
      }

      if (input.type === "group-route") {
        if (!Array.isArray(input.route) || input.route.length < 2 || input.route.length > 500) return;
        group.route = input.route;
        group.distance = routeDistance(group.route);
        broadcast({ type: "groups-state", groups: groupSnapshot() });
      }
    }
  });

  ws.on('close', () => {
    const driverId = connectionDrivers.get(ws.id);
    if (driverId && drivers.get(driverId)?.connectionId === ws.id) {
      drivers.delete(driverId);
      broadcast({ type: "location-stop", driverId });
    }
    connectionDrivers.delete(ws.id);
  });
});

console.log(`Servidor de Rumbo corriendo de forma estable en el puerto ${PORT}`);
