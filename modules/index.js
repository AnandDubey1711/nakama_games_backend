const CONSTANTS = {
  OP: {
    MOVE: 1,
    STATE: 2,
    ERROR: 3,
    ROOM_CREATED: 4,
    MATCH_START: 5,
    MATCH_END: 6,
    RECONNECT_WINDOW: 7,
  },
  DISCONNECT_GRACE_MS: 30000,
  RECONNECT_POLL_MS: 5000,
  MAX_PLAYERS_PER_ROOM: 2,
  ROOM_CODE_LENGTH: 8,
}

const PresenceStore = {}

function readOne(nk, collection, key, userId) {
  const req = { collection, key }
  if (userId) req.userId = userId
  const records = nk.storageRead([req])
  if (!records || !records.length) return null
  return records[0]
}

function writeOne(nk, collection, key, userId, value, version) {
  const req = {
    collection, key, value,
    permissionRead: 2, permissionWrite: 0,
  }
  if (userId) req.userId = userId
  if (version !== undefined) req.version = version
  try {
    nk.storageWrite([req])
  } catch (err) {}
}

function saveMatchResult(nk, matchId, winnerId, loserId) {
  try {
    nk.sqlQuery(
      "INSERT INTO match_results (match_id, winner_id, loser_id) VALUES ($1, $2, $3)",
      [matchId, winnerId, loserId]
    )
  } catch (err) {}
}

function createRoomRecord(nk, roomCode, creatorId) {
  const value = {
    roomCode,
    creatorId,
    players: [creatorId],
    started: false,
    createdAt: Date.now(),
  }
  writeOne(nk, "rooms", roomCode, null, value)
  return value
}

function getRoomByCode(nk, roomCode) {
  const record = readOne(nk, "rooms", roomCode, null)
  return record ? record.value : null
}

// Replaced by functions below

function getPlayerScore(nk, userId) {
  const record = readOne(nk, "scores", "score", userId)
  return record && record.value ? record.value : { wins: 0, losses: 0, points: 0 }
}

function updatePlayerScore(nk, userId, result) {
  let record = null
  try {
    record = readOne(nk, "scores", "score", userId)
  } catch (e) { }

  const current = (record && record.value) ? record.value : {
    wins: 0,
    losses: 0,
    points: 0,
  }
  const next = { ...current }
  if (result === "win") {
    next.wins += 1
    next.points += 3
  } else if (result === "draw") {
    next.points += 1
  } else if (result === "loss") {
    next.losses += 1
  }
  try {
    writeOne(nk, "scores", "score", userId, next, record ? record.version : undefined)
  } catch (e) {}
  return next
}

function createState(player1Id, player2Id) {
  return {
    board: Array(9).fill(null),
    currentTurn: player1Id,
    players: [player1Id, player2Id],
    status: "live",
    moveCount: 0,
    winner: null,
    disconnectedAt: {},
    roomCode: null,
  }
}

function validateMove(state, move, senderId) {
  if (state.status !== "live") return { valid: false, reason: "inactive" }
  if (senderId !== state.currentTurn) return { valid: false, reason: "turn" }
  if (!move || typeof move.index !== "number") return { valid: false, reason: "move" }
  if (move.index < 0 || move.index > 8) return { valid: false, reason: "bounds" }
  if (state.board[move.index]) return { valid: false, reason: "occupied" }
  return { valid: true }
}

const winLines = [
  [0, 1, 2],
  [3, 4, 5],
  [6, 7, 8],
  [0, 3, 6],
  [1, 4, 7],
  [2, 5, 8],
  [0, 4, 8],
  [2, 4, 6],
]

function findWinner(board) {
  for (let i = 0; i < winLines.length; i += 1) {
    const [a, b, c] = winLines[i]
    if (board[a] && board[a] === board[b] && board[a] === board[c]) {
      return board[a]
    }
  }
  return null
}

function applyMove(state, move) {
  const board = state.board.slice()
  const mark = move.playerId === state.players[0] ? "X" : "O"
  board[move.index] = mark
  const moveCount = state.moveCount + 1
  const nextTurn = state.players.find((id) => id !== move.playerId)
  return {
    ...state,
    board,
    moveCount,
    currentTurn: nextTurn,
  }
}

function checkResult(state) {
  const winner = findWinner(state.board)
  if (winner) return "win"
  if (state.moveCount >= 9) return "draw"
  return "ongoing"
}

function winnerId(state) {
  return findWinner(state.board)
}

function publicState(state) {
  return {
    board: state.board,
    currentTurn: state.currentTurn,
    players: state.players,
    status: state.status,
    moveCount: state.moveCount,
    winner: state.winner,
    disconnectedAt: state.disconnectedAt,
  }
}

function broadcast(dispatcher, presences, state, opCode) {
  const code = opCode || CONSTANTS.OP.STATE
  const payload = code === CONSTANTS.OP.STATE ? publicState(state) : state
  dispatcher.broadcastMessage(code, JSON.stringify(payload), presences, null)
}

function broadcastToOne(dispatcher, presence, opCode, payload) {
  const body = opCode === CONSTANTS.OP.STATE ? publicState(payload) : payload
  dispatcher.broadcastMessage(opCode, JSON.stringify(body), [presence], null)
}

function broadcastResult(dispatcher, presences, result) {
  const payload = {
    result,
    winnerId: result.winnerId || null,
    loserId: result.loserId || null,
  }
  dispatcher.broadcastMessage(CONSTANTS.OP.MATCH_END, JSON.stringify(payload), presences, null)
}

function randomRoomCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
  let code = ""
  for (let i = 0; i < CONSTANTS.ROOM_CODE_LENGTH; i += 1) {
    const index = Math.floor(Math.random() * chars.length)
    code += chars[index]
  }
  return code
}

function createRoom(nk, userId) {
  let code = randomRoomCode()
  let exists = getRoomByCode(nk, code)
  while (exists) {
    code = randomRoomCode()
    exists = getRoomByCode(nk, code)
  }
  createRoomRecord(nk, code, userId)
  return code
}

function joinRoom(nk, userId, roomCode) {
  const room = getRoomByCode(nk, roomCode)
  if (!room) return null
  if (room.started) return null
  if (room.players.length >= CONSTANTS.MAX_PLAYERS_PER_ROOM) return {} 
  if (!room.players.includes(userId)) {
    const updated = { ...room, players: [...room.players, userId] }
    updateRoomPlayers(nk, roomCode, updated.players, updated.started)
    return updated
  }
  return room
}

function isRoomReady(roomRecord) {
  return roomRecord.players.length === CONSTANTS.MAX_PLAYERS_PER_ROOM
}

function upsertPresence(matchId, presence) {
  const list = PresenceStore[matchId] || []
  const existing = list.find((item) => item.userId === presence.userId)
  if (!existing) list.push(presence)
  PresenceStore[matchId] = list
  return list
}

function removePresence(matchId, userId) {
  const list = PresenceStore[matchId] || []
  PresenceStore[matchId] = list.filter((item) => item.userId !== userId)
}

function getPresences(matchId) {
  return PresenceStore[matchId] || []
}

function matchJoinAttempt(ctx, logger, nk, dispatcher, tick, state, presence) {
  if (!state.roomCode) return { state, accept: true }
  const room = getRoomByCode(nk, state.roomCode)
  if (!room) return { state, accept: false }
  if (!room.players.includes(presence.userId)) return { state, accept: false }
  return { state, accept: true }
}

function matchJoin(ctx, logger, nk, dispatcher, tick, state, presences) {
  const disconnectedAt = { ...state.disconnectedAt }
  let players = state.players.slice()
  presences.forEach((presence) => {
    if (!players.includes(presence.userId)) {
      const openIndex = players.findIndex((id) => !id)
      if (openIndex >= 0) {
        players[openIndex] = presence.userId
      } else if (players.length < CONSTANTS.MAX_PLAYERS_PER_ROOM) {
        players.push(presence.userId)
      }
    }
  })
  presences.forEach((presence) => {
    upsertPresence(ctx.matchId, presence)
    delete disconnectedAt[presence.userId]
  })
  presences.forEach((presence) => {
    broadcastToOne(dispatcher, presence, CONSTANTS.OP.STATE, state)
  })
  const currentTurn = state.currentTurn || players[0]
  const nextState = { ...state, players, currentTurn, disconnectedAt }
  const allPresences = getPresences(ctx.matchId)
  if (allPresences.length === CONSTANTS.MAX_PLAYERS_PER_ROOM) {
    broadcast(dispatcher, allPresences, nextState, CONSTANTS.OP.MATCH_START)
  }
  return { state: nextState }
}

function matchLeave(ctx, logger, nk, dispatcher, tick, state, presences) {
  const disconnectedAt = { ...state.disconnectedAt }
  presences.forEach((presence) => {
    disconnectedAt[presence.userId] = Date.now()
    removePresence(ctx.matchId, presence.userId)
  })
  const nextState = { ...state, disconnectedAt }
  return { state: nextState }
}

function handleForfeit(nk, dispatcher, matchId, state, loserId) {
  const winner = state.players.find((id) => id !== loserId)
  if (!winner) return
  saveMatchResult(nk, matchId, winner, loserId)
  updatePlayerScore(nk, winner, "win")
  updatePlayerScore(nk, loserId, "loss")
  broadcastResult(dispatcher, getPresences(matchId), {
    winnerId: winner,
    loserId,
    outcome: "forfeit",
  })
}

function handleDraw(nk, dispatcher, matchId, state) {
  updatePlayerScore(nk, state.players[0], "draw")
  updatePlayerScore(nk, state.players[1], "draw")
  broadcastResult(dispatcher, getPresences(matchId), {
    winnerId: null,
    loserId: null,
    outcome: "draw",
  })
}

function handleWin(nk, dispatcher, matchId, state, winner) {
  const loserId = state.players.find((id) => id !== winner);
  saveMatchResult(nk, matchId, winner, loserId)
  updatePlayerScore(nk, winner, "win")
  updatePlayerScore(nk, loserId, "loss")
  broadcastResult(dispatcher, getPresences(matchId), {
    winnerId: winner,
    loserId,
    outcome: "win",
  })
}

function matchInit(ctx, logger, nk, params) {
  let players = params && params.players ? params.players : null
  if (!players && params && params.roomCode) {
    const room = getRoomByCode(nk, params.roomCode)
    players = room ? room.players : []
  }
  const player1Id = players && players.length > 0 ? players[0] : null
  const player2Id = players && players.length > 1 ? players[1] : null
  const state = createState(player1Id, player2Id)
  const nextState = { ...state, roomCode: params ? params.roomCode : null }
  return { state: nextState, tickRate: 10, label: "game" }
}

function matchLoop(ctx, logger, nk, dispatcher, tick, state, messages) {
  let nextState = state
  const now = Date.now()
  const disconnectedIds = Object.keys(state.disconnectedAt || {})
  for (let i = 0; i < disconnectedIds.length; i += 1) {
    const userId = disconnectedIds[i]
    const stamp = state.disconnectedAt[userId]
    if (now - stamp > CONSTANTS.DISCONNECT_GRACE_MS) {
      handleForfeit(nk, dispatcher, ctx.matchId, state, userId)
      return null
    }
  }

  for (let i = 0; i < messages.length; i += 1) {
    const message = messages[i]
    if (message.opCode !== CONSTANTS.OP.MOVE) continue
    let move = {}
    try {
      const payloadStr = (typeof message.data === "string") ? message.data : nk.binaryToString(message.data)
      move = JSON.parse(payloadStr)
    } catch (e) {
      move = {}
    }
    const senderId = message.sender.userId
    const validation = validateMove(nextState, move, senderId)
    if (!validation.valid) {
      broadcastToOne(dispatcher, message.sender, CONSTANTS.OP.ERROR, {
        reason: validation.reason,
      })
      continue
    }
    const applied = applyMove(nextState, {
      index: move.index,
      playerId: senderId,
    })
    const outcome = checkResult(applied)
    if (outcome === "ongoing") {
      nextState = applied
      broadcast(dispatcher, getPresences(ctx.matchId), nextState, CONSTANTS.OP.STATE)
    } else if (outcome === "win") {
      const winnerSymbol = winnerId(applied) 
      const winner = winnerSymbol === "X" ? applied.players[0] : applied.players[1]
      const finalState = { ...applied, status: "ended", winner }
      handleWin(nk, dispatcher, ctx.matchId, finalState, winner)
      return null
    } else if (outcome === "draw") {
      const finalState = { ...applied, status: "ended", winner: null }
      nextState = finalState
      handleDraw(nk, dispatcher, ctx.matchId, finalState)
      return null
    }
  }

  return { state: nextState }
}

function matchTerminate(ctx, logger, nk, dispatcher, tick, state, graceSeconds) {
  return { state }
}

function matchSignal(ctx, logger, nk, dispatcher, tick, state, data) {
  return { state, data: "" }
}

function parsePayload(payload) {
  if (!payload) return {}
  try {
    return JSON.parse(payload)
  } catch {
    return {}
  }
}

function rpcCreateRoom(ctx, logger, nk, payload) {
  const roomCode = createRoom(nk, ctx.userId)
  return JSON.stringify({ roomCode })
}

function rpcJoinRoom(ctx, logger, nk, payload) {
  const data = parsePayload(payload)
  const room = joinRoom(nk, ctx.userId, data.roomCode)
  if (!room) return JSON.stringify({ error: "room_unavailable" })
  return JSON.stringify({ roomCode: room.roomCode, players: room.players })
}

function markRoomActive(nk, roomCode, matchId) {
  const record = readOne(nk, "rooms", roomCode, null)
  if (!record) return null
  const updated = { ...record.value, started: true, matchId }
  writeOne(nk, "rooms", roomCode, null, updated, record.version)
  return updated
}

function updateRoomPlayers(nk, roomCode, players, started) {
  const record = readOne(nk, "rooms", roomCode, null)
  if (!record) return null
  const updated = { ...record.value, players, started: started || record.value.started }
  writeOne(nk, "rooms", roomCode, null, updated, record.version)
  return updated
}

function rpcStartMatch(ctx, logger, nk, payload) {
  const data = parsePayload(payload)
  let room = getRoomByCode(nk, data.roomCode)

  if (!isRoomReady(room)) {
    return JSON.stringify({ error: "room_not_ready" })
  }

  if (room.matchId) {
    return JSON.stringify({ matchId: room.matchId })
  }

  const matchId = nk.matchCreate("game", { roomCode: data.roomCode })
  markRoomActive(nk, data.roomCode, matchId)

  return JSON.stringify({ matchId })
}

function rpcGetPlayerScore(ctx, logger, nk, payload) {
  const score = getPlayerScore(nk, ctx.userId)
  return JSON.stringify(score)
}

function matchmakerMatched(ctx, logger, nk, matched) {
  var users = matched.users || []
  var players = users.map(function (u) { return u.presence.userId })
  var matchId = nk.matchCreate("game", { players: players })
  users.forEach(function (u) {
    nk.notificationSend(u.presence.userId, "match_found", { matchId: matchId }, 1, null, true)
  })
  return matchId
}

function InitModule(ctx, logger, nk, initializer) {
  // Create match_results table if it doesn't exist
  nk.sqlQuery(
    "CREATE TABLE IF NOT EXISTS match_results (" +
    "match_id VARCHAR(128) PRIMARY KEY, " +
    "winner_id VARCHAR(128), " +
    "loser_id VARCHAR(128), " +
    "created_at TIMESTAMPTZ DEFAULT now())",
    []
  )

  initializer.registerMatch("game", {
    matchInit: matchInit,
    matchJoinAttempt: matchJoinAttempt,
    matchJoin: matchJoin,
    matchLoop: matchLoop,
    matchLeave: matchLeave,
    matchTerminate: matchTerminate,
    matchSignal: matchSignal
  })

  initializer.registerRpc("createRoom", rpcCreateRoom)
  initializer.registerRpc("joinRoom", rpcJoinRoom)
  initializer.registerRpc("startMatch", rpcStartMatch)
  initializer.registerRpc("getPlayerScore", rpcGetPlayerScore)

  initializer.registerMatchmakerMatched(matchmakerMatched)
}
